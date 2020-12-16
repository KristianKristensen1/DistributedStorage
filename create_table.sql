CREATE TABLE IF NOT EXISTS `file` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `filename` TEXT,
   `size` INTEGER,
   `content_type` TEXT,
   `storage_mode` TEXT,
   `storage_details` TEXT,
   `replica_locations` TEXT,
   `recieved_time` INTEGER,   
   `created` DATETIME DEFAULT CURRENT_TIMESTAMP
);

