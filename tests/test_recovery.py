"""Offline regression tests. External services are replaced, never contacted."""
import ast
import importlib
import math
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

from src.review_policy import authorized_callback, matching_review_message, valid_strength


class PolicyTests(unittest.TestCase):
    def callback(self, sender=123, chat=123):
        return {"from": {"id": sender}, "message": {"chat": {"id": chat}}}

    def test_private_owner(self):
        self.assertTrue(authorized_callback(self.callback(), "123"))

    def test_foreign_sender_and_chat_rejected(self):
        self.assertFalse(authorized_callback(self.callback(sender=456), "123"))
        self.assertFalse(authorized_callback(self.callback(chat=456), "123"))
        self.assertFalse(authorized_callback({}, None))

    def test_group_requires_explicit_approver(self):
        self.assertFalse(authorized_callback(self.callback(chat=-100), "-100"))
        self.assertTrue(authorized_callback(self.callback(chat=-100), "-100", "123"))
        self.assertFalse(authorized_callback(self.callback(sender=456, chat=-100), "-100", "123"))

    def test_message_binding(self):
        self.assertTrue(matching_review_message({"telegram_message_id": 7}, 7))
        self.assertFalse(matching_review_message({"telegram_message_id": 7}, 8))
        self.assertFalse(matching_review_message({}, None))

    def test_missing_and_invalid_strength_never_pass(self):
        for value in [None, False, True, "4", 0, -1, 6, math.nan, math.inf, {}, []]:
            with self.subTest(value=value):
                self.assertIsNone(valid_strength(value))
        self.assertEqual(valid_strength(4), 4)


class IntegrationBoundaryTests(unittest.TestCase):
    """Import actual application modules against isolated external boundaries."""
    def setUp(self):
        import src
        self.pkg = src
        self.old_attrs = dict(vars(src))
        self.old_modules = {k: v for k, v in sys.modules.items() if k.startswith("src.")}
        self.config = types.ModuleType("src.config")
        for k, v in dict(
            TELEGRAM_CHAT_ID="123", TELEGRAM_APPROVER_ID=None, ENABLE_CARD_IMAGES=False,
            ENABLE_SOCIAL_PUBLISHING=False, ENABLE_STORAGE_CLEANUP=False
        ).items():
            setattr(self.config, k, v)
        self.db = MagicMock()
        self.stubs = {"src.config": self.config, "requests": MagicMock()}
        for name in ["supabase_client", "wastake_client", "card_generator", "entity_detector",
                     "rss_fetcher", "carousel_generator"]:
            m = types.ModuleType("src." + name)
            self.stubs["src." + name] = m
        self.stubs["src.supabase_client"].get_client = lambda: self.db
        self.stubs["src.rss_fetcher"].is_parasitic = lambda _: False
        self.stubs["src.wastake_client"].get_news_angle = MagicMock()
        self.stubs["src.card_generator"].render_and_upload = MagicMock()
        self.stubs["src.carousel_generator"].generate_carousel = MagicMock()
        self.stubs["src.carousel_generator"].upload_carousel_to_supabase = MagicMock()
        self.patch = patch.dict(sys.modules, self.stubs)
        self.patch.start()
        for name, m in self.stubs.items():
            if name.startswith("src."):
                setattr(src, name.split(".")[1], m)

    def tearDown(self):
        self.patch.stop()
        for k in list(sys.modules):
            if k.startswith("src.") and k not in self.old_modules:
                del sys.modules[k]
        for k in list(vars(self.pkg)):
            if k not in self.old_attrs:
                delattr(self.pkg, k)
        vars(self.pkg).update(self.old_attrs)

    def test_editorial_without_images_imports_configuration(self):
        m = importlib.import_module("src.editorial_generator")
        m.wastake_client.get_news_angle.return_value = {"angle": "context", "strength": 4}
        with patch.object(m, "_compose_post", return_value={"headline": "Fixture"}), patch.object(m, "_mark_candidate"):
            self.assertIsNone(m._process_one({"id": 1, "headline": "Fixture"}, None))
        m.card_generator.render_and_upload.assert_not_called()
        self.db.table.return_value.insert.assert_called_once()

    def test_invalid_editorial_strength_does_not_write_post(self):
        m = importlib.import_module("src.editorial_generator")
        m.wastake_client.get_news_angle.return_value = {"angle": "context", "strength": None}
        self.assertIn("invalid strength", m._process_one({"id": 1, "headline": "Fixture"}, None))
        self.db.table.assert_not_called()

    def test_unauthorized_callback_cannot_read_post_or_generate_prompts(self):
        m = importlib.import_module("src.telegram_bot")
        cb = {"id": "test", "data": "prompts:9", "from": {"id": 456},
              "message": {"message_id": 7, "chat": {"id": 123}}}
        with patch.object(m, "_get_state", return_value={"offset": 0}), patch.object(m, "_set_state"), patch.object(m, "_get_updates", return_value=[{"update_id": 1, "callback_query": cb}]), patch.object(m, "_handle_prompts_callback") as prompts:
            self.assertEqual(m.process_callbacks()["ignored"], 1)
            prompts.assert_not_called()
            self.db.table.assert_not_called()

    def test_second_approval_is_not_accepted_or_announced(self):
        m = importlib.import_module("src.telegram_bot")
        q = self.db.table.return_value.update.return_value
        q.eq.return_value = q
        q.execute.return_value.data = []
        with patch.object(m, "_edit_message_text") as edit:
            self.assertIn("ya no", m._apply_action("approve", 1, "Fixture", 123, 7))
            edit.assert_not_called()
        self.assertIn(unittest.mock.call("status", "pending_approval"), q.eq.call_args_list)

    def test_publishers_are_off_even_with_legacy_credentials(self):
        for name in ["twitter_publisher", "tiktok_publisher"]:
            m = importlib.import_module("src." + name)
            self.assertEqual(m.run_one_cycle()["published"], 0)
        self.db.table.assert_not_called()

    def test_recovery_defaults_do_not_enable_external_side_effects(self):
        # Execute configuration with a clean environment and inert dotenv.
        fake = types.ModuleType("dotenv")
        fake.load_dotenv = lambda: None
        namespace = {}
        with patch.dict("os.environ", {}, clear=True), patch.dict(sys.modules, {"dotenv": fake}):
            exec(Path("src/config.py").read_text(encoding="utf-8"), namespace)
        self.assertFalse(namespace["ENABLE_SOCIAL_PUBLISHING"])
        self.assertFalse(namespace["ENABLE_STORAGE_CLEANUP"])
        self.assertFalse(namespace["ENABLE_CARD_IMAGES"])
        self.assertEqual(namespace["WASTAKE_API_URL"], "https://wastake.onrender.com")

    def test_all_source_files_parse(self):
        for p in Path("src").glob("*.py"):
            with self.subTest(path=p):
                ast.parse(p.read_text(encoding="utf-8-sig"))


if __name__ == "__main__":
    unittest.main()
