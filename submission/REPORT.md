# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Thị Mừng/ 2A202602575
**Repo:**  https://github.com/mungan2004/K4-Track02-Day17-NguyenThiMung-2A202602575-DataPipelineEngineering.git
**Commit bài nộp:** a68bce349619b66195c8d7b085d069fa20d89d3d
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity IDE (Gemini) hỗ trợ phát hiện và sửa 3 lỗi
**Nguồn tham khảo khác (nếu có):** 

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ, checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Lỗi check count row: "silver_tickets has exactly one row per ticket_id" fail (24 rows for 12 tickets). | Check "u05's offline events of 08-12 (arrived 08-15) are counted on 08-12" bị báo (2, 0) thay vì (5, 1). | Check "latest training snapshot excludes the deleted ticket T-97" báo fail vì vẫn còn 1 dòng. |
| **Nguyên nhân gốc** | Dùng `INSERT INTO` khiến mỗi thay đổi sinh thêm 1 bản ghi mới thay vì cập nhật trạng thái mới nhất cho 1 khoá. | `LOOKBACK_DAYS` cấu hình cứng là 0, bỏ qua dữ liệu đến muộn (lateness data) P99 mất 3 ngày đo từ Bronze. | Ở file `staging.py`, lấy `ticket_id` từ `j->'value'->'after'` nhưng payload xoá (op = 'd') thì phần 'after' là null. |
| **Cách sửa** (file, vài dòng) | Trong `pipeline/silver.py`: Đổi `INSERT INTO silver_tickets ...` thành lệnh `MERGE INTO ... WHEN MATCHED THEN UPDATE`. | Trong `pipeline/config.py`: Đổi giá trị hằng số `LOOKBACK_DAYS = 0` thành `LOOKBACK_DAYS = 3`. | Trong `pipeline/staging.py`: Đổi lệnh lấy id thành `j->'key'->>'ticket_id'` vì key luôn tồn tại ở Debezium event. |
| **Khái niệm trên slide** | Bốn cách viết idempotent (Silver - Có khoá, 1 row = 1 entity). | Data về muộn / Lateness / Window Lookback. | Xoá phải lan (Propagation) / Tombstone / CDC log-based. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `8630e04a61d1`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE đảm bảo cập nhật trạng thái mới nhất theo từng entity (SCD1), trong khi overwrite-partition giúp tính lại toàn bộ aggregates của một ngày một cách idempotent dễ dàng.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giúp truy xuất lịch sử, phục vụ audit lineage và dễ dàng lan truyền hiệu ứng xoá (propagate) sang các mô hình Gold phụ thuộc.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Để giữ tính bất biến nhằm đảm bảo không rò rỉ dữ liệu tương lai (data leakage) và đảm bảo các thí nghiệm ML có thể tái lặp 100%.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu đủ gọn để chạy in-process cực nhanh, Spark sẽ gây tốn tài nguyên và overhead phân tán không cần thiết.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
**Trả lời:** Có thể che giấu thay vì xoá (Masking PII): áp dụng hàm mask định danh đối với toàn bộ bản ghi training ngay từ lúc sinh để không lưu thông tin nhạy cảm. Nếu quy định khắt khe, áp dụng Machine Unlearning để loại bỏ ảnh hưởng của T-97 hoặc chủ động phát hành phiên bản snapshot patch (ví dụ v2026-08-14_patched) và ngưng dùng bản cũ.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
**Trả lời:** Đặt chốt NLP Named Entity Recognition (NER) (như spaCy hoặc API LLM) ở ngay bước ghi từ Bronze lên Silver để nhận diện tên riêng. Để đo đạc, tạo Data Quality Gate tính tỷ lệ lọt lưới PII trên dữ liệu mẫu và chặn quarantine nếu phát hiện có chứa danh từ riêng chưa bị mask trong field text.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
============================= test session starts ==============================
...
============================= 4 passed in 0.05s ==============================

$ make rerun3
C0 = 8630e04a61d1 (fresh build)
C1 = 8630e04a61d1 (re-run 2026-08-12 #1)
C2 = 8630e04a61d1 (re-run 2026-08-12 #2)
C3 = 8630e04a61d1 (re-run 2026-08-12 #3)
PASS

$ make lateness
Lateness measured from Bronze (events):
  Count: 39
  P50:   0.00 days
  P95:   0.00 days
  P99:   3.00 days
  Max:   3.00 days
Recommendation: LOOKBACK_DAYS >= 3

$ make dbt
...
00:21:26  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.74 seconds (0.74s).
00:21:26  Completed successfully
00:21:26  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
