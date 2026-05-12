print("DB HAS", channel_conn.execute("SELECT count(*) FROM device_channel").fetchone())
