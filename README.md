
# Multi-Service Pub-Sub Log Aggregator

Sistem agregator log berbasis Microservices yang menerapkan pola Publisher-Subscriber. Sistem ini dirancang untuk mendemonstrasikan **Idempotency**, **Strong Deduplication**, **Concurrency Control**, dan **Data Persistence**.
## Arsitektur Sistem

Sistem terdiri dari 4 layanan utama yang berjalan dalam jaringan terisolasi (`bridge network`):

1.  **Publisher**: Generator event simulasi. Mengirimkan 20.000+ event (termasuk duplikat sengaja) ke Aggregator/Broker.
2.  **Aggregator**:
    * **API Layer (FastAPI)**: Menerima request HTTP dan menaruhnya ke antrean.
    * **Consumer Layer (Asyncio)**: Mengambil pesan dari Broker dan memprosesnya ke Database.
3.  **Broker (Redis)**: Message broker in-memory untuk antrean tugas (Queue).
4.  **Storage (PostgreSQL)**: Database relasional untuk persistensi data dengan constraint unik.

### Arsitektur
`Publisher post /publish` -> `Aggregator API` -> `Message Broker Redis` -> `Consumer worker` -> `PostgreSQL` -> `/stats /events`

---

## Instruksi Build & Run

### Prasyarat
* Docker Engine
* Docker Compose

### 1. Menjalankan Sistem
Build image dan jalankan container di background:
```bash
docker compose up --build -d

```

### 2. Scaling Worker Untuk menjalankan multiple consumer worker secara paralel (pastikan port di `docker-compose.yml` diset range `8080-8082:8080`):

```bash
docker compose up -d --scale aggregator=3

```

### 3. Melihat Logs untuk memantau aktivitas consumer dan publisher:

```bash
docker compose logs -f aggregator
# atau
docker compose logs -f publisher

```

### 4. Reset Total (Hapus Data) Untuk menghapus container beserta volume data:

```bash
docker compose down -v

```

---

## API EndpointsBase URL: `http://localhost:8080` (atau sesuaikan dengan range pada konfigurasi compose port 8080/8081/8082)

### 1. Get System Stats melihat jumlah antrean, dan metrik deduplikasi.

* **Method**: `GET`
* **Endpoint**: `/stats`
* **Response**:
```json
{
  "total_received_queued": 20000,
  "unique_processed_db": 18257,
  "estimated_duplicate_dropped": 1743,
  "uptime_seconds": 52.3
}

```



### 2. Publish Event (Single)Mengirim satu event ke sistem.

* **Method**: `POST`
* **Endpoint**: `/publish`
* **Body**:
```json
{
  "topic": "order",
  "event_id": "unique-uuid-123",
  "timestamp": "2025-01-01T12:00:00Z",
  "source": "manual",
  "payload": { "amount": 100 }
}

```



### 3. Get Processed EventsMelihat daftar event yang berhasil disimpan (unik).

* **Method**: `GET`
* **Endpoint**: `/events`
* **Query Param**: `?topic=order` (Opsional)

---

## Testing 
### A. Menjalankan Unit & Integration Test.

```bash
docker compose exec aggregator pytest /tests/test_integration.py -v

```

### B. Uji Idempotency & Deduplikasi
1. Kirim event dengan `topic` dan `event_id` yang sama persis sebanyak 5 kali via Postman/Curl.

2. Cek endpoint `/stats`.

3. **Hasil yang diharapkan**: `unique_processed_db` bertambah 1, `estimated_duplicate_dropped` bertambah 4.

### C. Uji Persistensi (Crash & Recovery)
1. Biarkan sistem memproses data.
2. Matikan paksa: `docker compose down` (tanpa `-v`).
3. Nyalakan kembali: `docker compose up -d`.
4. Cek `/stats` atau `/events`.
5. **Hasil yang diharapkan**: Data lama masih tersedia dan jumlahnya tidak reset ke 0.

### D. Uji Konkurensi (Race Condition)
1. Jalankan 3 worker: 
`docker compose up -d --scale aggregator=3`.

2. Kirim 50 request konkuren dengan `event_id` yang sama (gunakan Postman Runner atau JMeter).

3. **Hasil yang diharapkan**: Hanya 1 data tersimpan di DB, tidak ada error *Duplicate Key* yang menyebabkan crash (ditangani oleh `ON CONFLICT DO NOTHING`).

---

## Desain Teknis
1. **Deduplikasi**:
Menggunakan *Database Unique Constraint* `(topic, event_id)` sebagai *source of truth*. Aplikasi menggunakan strategi `INSERT ... ON CONFLICT DO NOTHING` untuk menangani duplikasi secara atomik di level database.

2. **Isolasi Transaksi**:
Menggunakan default PostgreSQL `READ COMMITTED`. Karena deduplikasi ditangani oleh index constraint unik, level isolasi ini cukup untuk mencegah race condition tanpa perlu `SERIALIZABLE` yang lebih berat.

3. **Publisher Behavior**:
Container Publisher didesain untuk mengirim tepat 20.000 event lalu berhenti (*graceful exit*) untuk memudahkan validasi statistik.

---

## Video YouTube

https://youtu.be/GB_RH1ALuHg