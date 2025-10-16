---

### File States:

* **NEW** – new file, requires upload to FTP
* **SYNCED** – file is synchronized (S3 and FTP are identical)
* **DELETED_ON_S3** – file is deleted in S3, requires removal from FTP
* **TEMP_DELETED** – temporary state for processing
* **ERROR** – synchronization error

---

### Main Algorithm:

#### Scan Start:

**Preparation:**

* All files with status **SYNCED** → set to **TEMP_DELETED**
* Files in **DELETED_ON_S3** remain unchanged (a previous deletion attempt may not have finished)
* Files in **NEW** remain unchanged (interrupted previous session)

**S3 Scan and State Update:**

* For each file in S3:

  * If not in cache → **NEW**
  * If found in cache:

    * Status **TEMP_DELETED**:

      * If hash matches → **SYNCED** (file unchanged)
      * If hash differs → **NEW** (file changed)
    * Status **NEW** → stays **NEW**
    * Status **DELETED_ON_S3** → **NEW** (file reappeared)

**Final cache state adjustment:**

* If status was **TEMP_DELETED** → change to **DELETED_ON_S3** (not found in S3 → considered deleted)
* All other statuses remain unchanged

---

### Synchronization:

* Process **NEW** → upload to FTP → on success → **SYNCED**
* Process **DELETED_ON_S3** → delete from FTP → on success → remove from cache
* In case of an error → status remains unchanged

---

| Previous State | Found in S3 | ETag Same? | New State         |
| -------------- | ----------- | ---------- | ----------------- |
| SYNCED         | ❌           | —          | TEMP_DELETED      |
| TEMP_DELETED   | ❌           | —          | DELETED_ON_S3     |
| TEMP_DELETED   | ✅           | ✅          | SYNCED            |
| TEMP_DELETED   | ✅           | ❌          | NEW               |
| DELETED_ON_S3  | ✅           | —          | NEW               |
| NEW            | ✅           | —          | NEW (update ETag) |

---
