"""Pure validation rules; no environment, network or database access."""
import math


def valid_strength(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) and 1 <= value <= 5 else None


def authorized_callback(callback, chat_id, approver_id=None):
    message = callback.get("message") or {}
    actual_chat = (message.get("chat") or {}).get("id")
    sender = (callback.get("from") or {}).get("id")
    # A private chat's owner is its positive chat ID. Groups need an explicit
    # approver; belonging to the group alone never grants editorial authority.
    owner = approver_id or (chat_id if str(chat_id or "").isdigit() else None)
    return bool(
        owner and chat_id and sender and actual_chat
        and str(actual_chat) == str(chat_id)
        and str(sender) == str(owner)
    )


def matching_review_message(post, message_id):
    expected = post.get("telegram_message_id")
    return bool(expected and message_id and str(expected) == str(message_id))
