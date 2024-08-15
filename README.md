# ETL Pipeline with Luigi

## Problem

---

- Tim Sales ini melakukan analisis mengenai pengaruh harga diskon dan harga jual terhadap rating produk. Mereka sudah mempunyai data penjualan barang di Database PostgreSQL. Namun masih banyak data mereka yang kosong dan formatnya belum benar sehingga menyulitkan mereka untuk melakukan analisis.
- Tim Product ingin melakukan analisis mengenai pengaruh berat produk terhadap harga. Mereka sudah mempunyai datanya dalam bentuk csv tetapi mereka ingin data tersebut berada terpusat di database agar mudah untuk diambil. Selain itu data mereka memiliki format data yang masih berantakan dan banyak missing value.
- Tim Data Scientist ingin melakukan research mengenai cara meringkas berita dengan menggunakan NLP, namun mereka belum memiliki data berita sama sekali.

## Solution

---

- Requirements

  - Make requirements for the data value and data format
  - Requirements Tools: Python, Pandas, Luigi, VS code, and Postgresql

- Extract

  - Collect the data
  - Understanding the data
  - Check data types and values

- Transform
  - Drop unusable column
  - Rename Columns
  - Casting columns
  - Cleaning the data value
  - Formatting the table
  -
- Load

  - Save the output of cleaned sales data to sales_data table on posgresql
  - Save the output of cleaned marketing data to marketing_data table on posgresql
  - Save the output of cleaned the news data table on posgresql
  - All the data cleaned is saved in the same database run by Docker
  - Automate the task
  - Make a scheduler task with cronjob

- Extract

  - Kumpulkan data dari berbagai sumber
  - Pahami konteks dan business dari data
  - Cek tipe data dan valuenya

- Transform

  - Buang kolom yang tidak digunakan
  - Rename Columns
  - Casting (mengubah tipe kolom) columns
  - Cleaning setiap row data
  - Formatting the data table

- Load

  - Simpan output dari data sales yang sudah dibersihkan ke dalam database PostgreSQL
  - Simpan output dari data Marketing yang sudah dibersihkan ke dalam database PostgreSQL
  - Simpan output dari data Scraping berita online yang sudah dibersihkan ke dalam database PostgreSQL
  - Semua table yang sudah dibersihkan tersebut disimpan dalam database yang sama yang dijalankan oleh Docker
  - Otomasi semua task di atas
  - Jadwalkan otomasi semua task di atas dengan Crontab

    ![ETL Pipeline Diagram](./asset/ETL-pipeline-Flow.png)
