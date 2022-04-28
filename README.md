# How to do rollback to $height

1. Stop the consumer;

2. Execute SQL `SELECT rollback_to($height);` (function `rollback_to` provided via the migrations);

3. Invalidate cache using `invalidate` binary or through the `admin` service - `/admin/cache/invalidate?mode=all_data` endpoint;

4. Start the consumer.
