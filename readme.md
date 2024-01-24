# T-SCRAPY
### Deskripsi
- t-scrapy adalah python script yang dibangun untuk mempermudah proses "scraping" data shop tokopedia
- skrip ini di desain dengan otomatisasi menngambil data produk dan review. serta membuat, menambah dan meng-update pada database (t-scrapy.db)
- t-scrapy mengambil input url shop dari tokopedia (lihat gambar dibawah)
![shop url](https://github.com/ekasetyo090/T-SCRAPY/blob/71be2fb5e1bbc19fe8972a76015dd49c5c1dd725/screenshot/shop%20url.png)

### motivasi
pada umumnya scraping website akan menggunakan library beatifulsoup atau selenium, penulis merasa 2 cara tersebut kurang sudah sedikit "deprecated"
mengingat kebanyakan website sudah menerapkan API pada backend websitenya terutama e-commerce seperti tokopedia.
selain itu beautifulsoup membutuhkan bot untuk melakukan loading website, sedangkan backend API tidak memerlukan bot.
selain itu beautifulsoup hanya dapat mengambil data yang divisualisasi di website, data yang tidak ditampilkan tidak akan dapat di ambil menggunakan beatifulsoup
dengan memanfaatkan backend API kita dapat mengambil lebih banyak data yang tidak tampak seperti data "isLeasing"(produk yang dapat dicicl) pada tokopedia yang berbentuk bolean.
ditambah kelebihan backend API yang terstruktur memudahkan untuk melakukan scraping data produk, data toko, data campaign, dan data review.

### Instalasi dan penggunaan
1. unduh scrapy.py secara manual atau dengan mengetik "git clone https://github.com/ekasetyo090/T-SCRAPY"
2. Install dependency yang dibutuhkan (tercatat pada requirements.txt) dengan cara ketik "pip install requirements.txt" pada terminal
3. jalankan t-scrapy dengan ketik "python t-scrapy.py" pada terminal
4. anda akan diminta memasukkan url shop tokopedia(seperti di deskripsi)
5. tunggu proses selesai

### Cara kerja skrip
- skrip akan meminta url shop tokopedia yang akan di scraping
- skrip akan memeriksa keberadaan database (t-scrapy.db) jika database tidak ditemukan makan skrip akan membuat database baru
- skrip akan melakukan request ke graphQL API tokopedia.
- skrip akan terlebih dahulu mengambil data sebagai berikut
- 1. data toko,
- 2. data list produk
- 3. data produk
- 4. data reviews

