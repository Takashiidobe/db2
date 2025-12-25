# Storage and Serialization

## Page Layout

Fixed-size pages of 8 KiB (`PAGE_SIZE = 8192`) use a slotted page design:

### Page Structure
```
┌─────────────────────────────────────────────┐
│ Header (10 bytes)                           │
├─────────────────────────────────────────────┤
│ Slot Directory (grows downward)            │
│   [offset: u16, length: u16] per row       │
├─────────────────────────────────────────────┤
│ Free Space                                  │
├─────────────────────────────────────────────┤
│ Row Data (grows upward from page end)      │
└─────────────────────────────────────────────┘
```

### Header Format (10 bytes)
- 2 bytes: page type (`PageType`)
  - `0` = Heap page (table data)
  - `1` = B+Tree internal node (unused - B+Trees are in-memory)
  - `2` = B+Tree leaf node (unused)
- 4 bytes: page ID (u32)
- 2 bytes: number of rows (u16)
- 2 bytes: free space offset (u16, points to start of row data region)

### Slot Directory
- Grows downward from header
- Each entry: 4 bytes (offset: u16, length: u16)
- Indexed by slot ID (0-based)
- Provides indirection for variable-length rows

### Row Storage
- Rows are stored bottom-up (from end of page toward header)
- Variable-length encoding per row
- No padding or alignment requirements
- When a page is full (insufficient free space), a new page is allocated

## Disk Manager and Buffer Pool

### DiskManager (`src/storage/file.rs`)
Handles raw page I/O:
- `read_page(page_id)` - Read a page from disk at byte offset `page_id * PAGE_SIZE`
- `write_page(page)` - Write a page to disk with `sync_data()` for durability
- `allocate_page(page_type)` - Append a new page to the file
- `num_pages()` - Get total page count from file length

### BufferPool (`src/storage/buffer_pool.rs`)
In-memory page cache with LRU eviction:
- **Frames**: Fixed-size array of page slots
- **Page table**: HashMap mapping page_id → frame_id
- **LRU list**: Tracks access order for eviction policy
- **Pin counting**: Prevents eviction of in-use pages
- **Dirty tracking**: Marks modified pages for write-back

Operations:
- `fetch_page(page_id)` - Load page into buffer pool (from disk if not cached), pin it, mark as recently used
- `new_page(page_type)` - Allocate a new page on disk and fetch it
- `unpin_page(page_id, is_dirty)` - Decrease pin count, optionally mark dirty
- `flush_page(page_id)` - Write a dirty page to disk
- `flush_all()` - Write all dirty pages to disk (called on `.exit`)

Eviction policy:
1. Try to find an empty frame
2. If none, use LRU: iterate from least recently used to most recently used
3. Evict first unpinned frame (pin_count = 0)
4. If all frames are pinned, return error

## Heap Tables

Each table is a heap file `<name>.db` with unordered rows.

### Metadata Page (Page 0)
- Slot 0: `TABLE:<name>\n` (UTF-8 string)
- Slot 1: Serialized schema

Schema encoding:
```
[u16: column_count]
for each column:
  [u32: name_length][name_bytes][u8: type_tag]
```

Type tags:
- `0` = INTEGER
- `1` = VARCHAR (String)
- `2` = BOOLEAN

### Data Pages (Page 1+)
- Heap-organized rows in insertion order
- No particular ordering or clustering
- Pages are filled sequentially: when a page is full, allocate a new one

### Operations
- `create(name, schema, path, buffer_pool_size)` - Create a new table with metadata page
- `open(path, buffer_pool_size)` - Open existing table, read schema from page 0
- `insert(row)` - Validate row against schema, serialize, append to last page or allocate new page. Returns `RowId(page_id, slot_id)`.
- `get(row_id)` - Fetch page, read row from slot, deserialize
- `flush()` - Flush all dirty pages

### TableScan (`src/table/scan.rs`)
Sequential iterator over all rows:
- Starts at page 1 (skips metadata page 0)
- Iterates through pages in order, then slots within each page
- Stops when reaching EOF (page read fails with UnexpectedEof)
- Returns `(RowId, Vec<Value>)` per row

## Row and Column Encoding

### Row Serialization (`src/serialization/row.rs`)
Schema-driven encoding without type tags:

```
[u16: column_count]
for each value:
  INTEGER:  [i64: 8 bytes little-endian]
  BOOLEAN:  [u8: 0 or 1]
  VARCHAR:  [u32: length][utf8_bytes]
```

The schema provides type information, so rows don't need to embed type tags. This is space-efficient and used for all table data.

### Column Serialization (`src/serialization/column.rs`)
Self-describing format for homogeneous columns (used in tests):

```
[u32: value_count]
[u8: type_tag]
for each value:
  INTEGER:  [i64: 8 bytes little-endian]
  BOOLEAN:  [u8: 0 or 1]
  VARCHAR:  [u32: length][utf8_bytes]
```

All values in a column must have the same type. This format includes a type tag for self-description.

## Persistence

### On INSERT
- Rows are appended to heap pages
- Pages are marked dirty in buffer pool
- No immediate flush (write-back caching)

### On .exit
- `Executor::flush_all()` writes all dirty pages via `BufferPool::flush_all()`
- DiskManager calls `sync_all()` to ensure durability

### On Startup
- Scan `./data` for `.db` files
- For each file, open with `HeapTable::open()`
  - Read page 0 to extract table name and schema
  - Add to table catalog
- Read `./data/indexes.meta` for index definitions
- Rebuild in-memory B+Tree indexes by scanning table data with `TableScan`

## Design Rationale

### Why Fixed-Size Pages?
- Simplifies I/O (always read/write 8 KiB blocks)
- Aligns with OS page cache
- Standard in most database systems

### Why Slotted Pages?
- Supports variable-length rows without fragmentation
- Slot directory provides indirection (can move row data within page without changing RowId)
- Efficient space utilization

### Why Buffer Pool?
- Reduces disk I/O for frequently accessed pages
- Provides write-back caching (batch writes)
- Pin counting ensures safety for concurrent operations (though concurrency is not yet implemented)

### Why LRU Eviction?
- Simple and effective for most workloads
- Assumes recently accessed pages will be accessed again soon
- Alternative policies (CLOCK, 2Q) could be added later
