export function store(db) {
  const statement = (sql, args) => db.prepare(sql).bind(...args);
  const first = (sql, ...args) => statement(sql, args).first();
  const run = (sql, ...args) => statement(sql, args).run();
  const all = async (sql, ...args) => (await statement(sql, args).all()).results;
  const insertStatement = (story, now, editorial) => statement(
    'INSERT OR IGNORE INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,assets,created_at,state,editorial) SELECT ?,?,?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS(SELECT 1 FROM stories WHERE source_url=?)',
    [story.id, story.eventKey, story.title, story.source, story.url, story.published, story.priority,
      story.snippet, JSON.stringify(story.assets), now, editorial ? 'ready' : 'new',
      editorial ? JSON.stringify(editorial) : null, story.url]);
  return {
    first, run, all,
    async lock(name, owner, now, ttl) {
      return Boolean(await first(
        'INSERT INTO job_lock(name,owner,until_ms) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET owner=excluded.owner,until_ms=excluded.until_ms WHERE job_lock.until_ms<=? RETURNING name',
        name, owner, now + ttl, now));
    },
    unlock: (name, owner) => run('UPDATE job_lock SET until_ms=0 WHERE name=? AND owner=?', name, owner),
    set: (key, value) => run('INSERT INTO runtime(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', key, JSON.stringify(value)),
    async reserve(day, limit) {
      await run('INSERT OR IGNORE INTO daily_budget(day,used) VALUES(?,0)', day);
      return Boolean(await first('UPDATE daily_budget SET used=used+1 WHERE day=? AND used<? RETURNING used', day, limit));
    },
    async insert(story, now, editorial) {
      return insertStatement(story, now, editorial).run();
    },
    async insertMany(stories,now) {
      if(!stories.length)return 0;
      const results=await db.batch(stories.map(story=>insertStatement(story,now)));
      return results.reduce((n,r)=>n+(r.meta?.changes||0),0);
    },
    next: (now, cutoff) => first(
      "SELECT * FROM stories WHERE (state='new' OR (state='processing' AND lease_until<=?)) AND retry_at<=? AND published_at>=? AND attempts<3 ORDER BY priority DESC,published_at DESC LIMIT 1",
      now, now, cutoff),
    claim: (id, now) => first(
      "UPDATE stories SET state='processing',lease_until=?,attempts=attempts+1 WHERE id=? AND (state='new' OR (state='processing' AND lease_until<=?)) RETURNING *",
      now + 120_000, id, now),
    ready: (id, editorial) => run("UPDATE stories SET state='ready',editorial=?,lease_until=0 WHERE id=? AND state='processing'", JSON.stringify(editorial), id),
    ignore: id => run("UPDATE stories SET state='ignored',lease_until=0 WHERE id=? AND state='processing'", id),
    retry: (id, now, attempts) => run(
      "UPDATE stories SET state=?,retry_at=?,lease_until=0 WHERE id=? AND state='processing'",
      attempts >= 3 ? 'failed' : 'new', now + 30 * 60_000, id),
    notification: (id, force = false) => first(
      "UPDATE stories SET notification='sending' WHERE id=? AND state IN ('ready','pending') AND (notification='new' OR ?=1) RETURNING *", id, force ? 1 : 0),
    notified: (id, message) => run("UPDATE stories SET state='pending',notification='sent',message_id=? WHERE id=?", String(message), id),
    uncertain: id => run("UPDATE stories SET notification='uncertain' WHERE id=? AND notification='sending'", id),
    review: (id, message, approved, now) => first(
      "UPDATE stories SET state=?,approved_at=? WHERE id=? AND state='pending' AND message_id=? RETURNING *",
      approved ? 'approved' : 'rejected', approved ? now : null, id, String(message)),
    claimPack: (id, force = false) => first(
      "UPDATE stories SET pack_delivery='sending' WHERE id=? AND state='approved' AND (pack_delivery='new' OR ?=1) RETURNING *", id, force ? 1 : 0),
    packDone: id => run("UPDATE stories SET pack_delivery='sent' WHERE id=?", id),
    packUncertain: id => run("UPDATE stories SET pack_delivery='uncertain' WHERE id=? AND pack_delivery='sending'", id),
    queueUpdate: (update, now) => run('INSERT OR IGNORE INTO inbox(update_id,payload,created_at) VALUES(?,?,?)', update.update_id, JSON.stringify(update), now),
    claimUpdate: now => first(
      "UPDATE inbox SET state='processing',lease_until=?,attempts=attempts+1 WHERE update_id=(SELECT update_id FROM inbox WHERE (state='new' OR (state='processing' AND lease_until<=?)) AND attempts<3 ORDER BY update_id LIMIT 1) RETURNING *",
      now + 60_000, now),
    finishUpdate: id => run("UPDATE inbox SET state='done',lease_until=0 WHERE update_id=?", id),
    failUpdate: (id, attempts, now) => run("UPDATE inbox SET state=?,lease_until=? WHERE update_id=?", attempts >= 3 ? 'failed' : 'processing', now + 60_000, id)
  };
}
