# Clojure Memcached

by Samuel Hughes

Follows memcached protocol. Uses Sockets from Java library. 

# Basic example: 
  
    (use 'clojure.memcached)
    (let [sockets (setup-sockets ["localhost:11211", "localhost:11212"])]
       (set-val "key" "value")
       (get-val "key")
       (close-sockets sockets))

# Additional notes:

Anytime you call set-val, the key/value will be randomly distributed among all servers
that were originally passed into setup-memcached. It uses a hash algorithm, so it always
returns the same server based on the key.
