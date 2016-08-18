import digdag

def store_v(value):
  digdag.env.store({'v': value})

