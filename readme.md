# T-SCRAPY
### DESKRIPSI
- t-scrapy adalah python script yang dibangun untuk mempermudah proses "scraping" data shop tokopedia
- skrip ini di desain dengan otomatisasi menngambil data produk dan review. serta membuat, menambah dan meng-update pada database (t-scrapy.db)
- t-scrapy mengambil input url shop dari tokopedia (lihat gambar dibawah)
![shop url](https://github.com/ekasetyo090/T-SCRAPY/blob/71be2fb5e1bbc19fe8972a76015dd49c5c1dd725/screenshot/shop%20url.png)
- jika input bukan merupakan valid tokopedia shop url maka skrip akan menolak input dan meminta valid url kembali 

### MOTIVASI
#### PEMILIHAN METODE
pada umumnya scraping website akan menggunakan library beatifulsoup atau selenium, penulis merasa 2 cara tersebut kurang sudah sedikit "deprecated"
mengingat kebanyakan website sudah menerapkan API pada backend websitenya terutama e-commerce seperti tokopedia.
selain itu beautifulsoup membutuhkan bot untuk melakukan loading website, sedangkan backend API tidak memerlukan bot.
selain itu beautifulsoup hanya dapat mengambil data yang divisualisasi di website, data yang tidak ditampilkan tidak akan dapat di ambil menggunakan beatifulsoup
dengan memanfaatkan backend API kita dapat mengambil lebih banyak data yang tidak tampak seperti data "isLeasing"(produk yang dapat dicicl) pada tokopedia yang berbentuk bolean.
ditambah kelebihan backend API yang terstruktur memudahkan untuk melakukan scraping data produk, data toko, data campaign, dan data review.

#### ALASAN PEMBUATAN
penulis yang memiliki ketertarikan pada dunia "data analytics" merasa akan sangat mudah apabila mempunyai scraper 
untuk e-commerce tokopedia yang dapat menyimpang data yang sudah discraping kedalam SQL (pada kasus ini penulis menggunakan SQLite),
sehingga tidak perlu lagi menyimpan kedalam file CSV yang berbeda dan lebih efisien untuk menggunakannya dilain waktu ketika dibutuhkan.


### INSTALASI dan PENGGUNAAN
1. unduh scrapy.py secara manual atau dengan mengetik "git clone https://github.com/ekasetyo090/T-SCRAPY"
2. Install dependency yang dibutuhkan (tercatat pada requirements.txt) dengan cara ketik "pip install requirements.txt" pada terminal
3. jalankan t-scrapy dengan ketik "python t-scrapy.py" pada terminal
4. anda akan diminta memasukkan url shop tokopedia(seperti di deskripsi)
5. tunggu proses selesai

### CARA KERJA
- skrip akan meminta url shop tokopedia yang akan di scraping
- skrip akan memeriksa keberadaan database (t-scrapy.db) jika database tidak ditemukan makan skrip akan membuat database baru
- skrip akan melakukan request ke graphQL API tokopedia.
- skirp akan melakukan menambah data jika belum tercatat, mengupdate data bila sudah tercatat
- skrip akan terlebih dahulu mengambil data sebagai berikut
- 1 data toko,
- 2 data list produk
- 3 data produk
- 4 data reviews

### Sample Database
buka database t-scrapy.db untuk melihat data shop (https://www.tokopedia.com/votevip) yang telah dikumpulkan


### Struktur Database

Skrip ini menggunakan database SQLite dengan struktur tabel sebagai berikut:

#### Tabel Shop Information (`shop_info`)

- `description` (TEXT): Deskripsi toko.
- `domain` (TEXT): Domain toko pada Tokopedia.
- `shopId` (INTEGER): ID unik toko.
- `name` (TEXT): Nama toko.
- `nameSearch` (TEXT): Nama toko yang dapat dicari.(dalam bentuk lower case)
- `tagline` (TEXT): Tagline toko.
- `defaultSort` (TEXT): Pengurutan default produk di toko. (SQLite tidak support datetime type data)
- `dateRecorded` (TEXT): Tanggal rekaman data. (SQLite tidak support datetime type data)
- `dateOpen` (TEXT): Tanggal toko dibuka. (SQLite tidak support datetime type data)
- `totalFavorite` (INTEGER): Jumlah toko yang difavoritkan.
- `alreadyFavorited` (INTEGER): Jumlah kali toko difavoritkan.
- `activeProduct` (INTEGER): Jumlah produk aktif di toko.
- `avatar` (TEXT): URL avatar toko.
- `cover` (TEXT): URL cover toko.
- `location` (TEXT): Lokasi toko.
- `isAllowManage` (INTEGER): Izin untuk mengelola toko.
- `branchLinkDomain` (TEXT): Domain untuk cabang toko.
- `isOpen` (TEXT): Status toko (buka/tutup).
- `shippingOptions` (TEXT): Opsi pengiriman yang ditawarkan.
- `shippingDistrict` (TEXT): Daerah pengiriman.
- `shippingCity` (TEXT): Kota pengiriman.
- `soldProducts` (INTEGER): Jumlah produk yang terjual.
- `totalSuccessTransactions` (INTEGER): Total transaksi sukses.
- `totalShowcase` (INTEGER): Total Showcase.
- `closedNote` (TEXT): Catatan jika toko ditutup.
- `closedUntil` (TEXT): Tanggal hingga toko ditutup.
- `closedReason` (TEXT): Alasan penutupan toko.
- `isGoldShop` (INTEGER): Status Gold Shop. (0:FALSE,1:TRUE)
- `isGoldBadgeShop` (INTEGER): Status Gold Badge Shop. (0:FALSE,1:TRUE)
- `isOfficialShop` (INTEGER): Status Official Shop. (0:FALSE,1:TRUE)
- `shopSnippetUrl` (TEXT): URL cuplikan toko.
- `seoTitle` (TEXT): Judul SEO toko.
- `seoDescription` (TEXT): Deskripsi SEO toko.
- `seoContentBottom` (TEXT): Konten SEO bagian bawah.
- `isQA` (INTEGER): Status Quality Assurance. (0:FALSE,1:TRUE)
- `isGoApotik` (INTEGER): Status toko GoApotik. (0:FALSE,1:TRUE)

#### Tabel Active Product List (`active_product_list`)

- `dateRecorded` (TEXT): Tanggal rekaman data. (SQLite tidak support datetime type data)
- `shopId` (INTEGER): ID unik toko.
- `productName` (TEXT): Nama produk.
- `productUrl` (TEXT): URL produk.
- `productId` (INTEGER): ID unik produk.
- `priceText` (REAL): Harga produk.
- `discounted_percentage` (INTEGER): Persentase diskon.
- `original_price` (REAL): Harga asli produk.
- `start_date` (TEXT): Tanggal mulai diskon. (SQLite tidak support datetime type data)
- `end_date` (TEXT): Tanggal akhir diskon. (SQLite tidak support datetime type data)
- `imageOriginal` (TEXT): URL gambar asli.
- `imageThumbnail` (TEXT): URL gambar thumbnail.
- `imageResize300` (TEXT): URL gambar ukuran 300.
- `isSold` (INTEGER): Status produk terjual. (0:FALSE,1:TRUE)
- `isPreorder` (INTEGER): Status produk pra-pesan. (0:FALSE,1:TRUE)
- `isWholesale` (INTEGER): Status produk grosir. (0:FALSE,1:TRUE)
- `storeBadge` (TEXT): Badge toko.
- `reviewCount` (INTEGER): Jumlah ulasan produk.
- `rating` (INTEGER): Rating produk.
- `averageRating` (REAL): Rating rata-rata produk.
- `category` (TEXT): Kategori produk.
- `shopDomain` (TEXT): Domain toko.
- `productKey` (TEXT): Kunci produk.
- `extParam` (TEXT): Parameter tambahan (Untuk parameter SCRAPING).

#### Tabel Detailed Shop Product List (`detailed_shop_product_list`)

- `alias` (TEXT): Alias produk.
- `createdAt` (TEXT): Tanggal pembuatan produk. (SQLite tidak support datetime type data)
- `dateRecorded` (TEXT): Tanggal rekaman data. (SQLite tidak support datetime type data)
- `isQA` (INTEGER): Status Quality Assurance. (0:FALSE,1:TRUE)
- `productId` (INTEGER): ID unik produk.
- `shopId` (TEXT): ID unik toko.
- `shopName` (TEXT): Nama toko.
- `minOrder` (INTEGER): Jumlah pesanan minimum.
- `maxOrder` (INTEGER): Jumlah pesanan maksimum.
- `weight` (INTEGER): Berat produk.
- `weightUnit` (TEXT): Satuan berat produk.
- `condition` (TEXT): Kondisi produk.
- `status` (TEXT): Status produk.
- `productUrl` (TEXT): URL produk.
- `isNeedPrescription` (INTEGER): Status resep dokter dibutuhkan.
- `catalogId` (TEXT): ID katalog produk.
- `isLeasing` (INTEGER): Status produk bisa dicicil. (0:FALSE,1:TRUE)
- `isBlacklisted` (INTEGER): Status produk masuk daftar hitam. (0:FALSE,1:TRUE)
- `isTokoNow` (INTEGER): Status produk Toko Now. (0:FALSE,1:TRUE)
- `etalaseId` (TEXT): ID etalase produk.
- `etalaseName` (TEXT): Nama etalase produk.
- `etalaseUrl` (TEXT): URL etalase produk.
- `categoryId` (TEXT): ID kategori produk.
- `categoryName` (TEXT): Nama kategori produk.
- `categoryTitle` (TEXT): Judul kategori produk.
- `categoryUrl` (TEXT): URL kategori produk.
- `isAdult` (TEXT): Status produk dewasa. (Penulis masih tidak yakin dengan jenis data)
- `isKyc` (TEXT): Status produk KYC. (Penulis masih tidak yakin dengan jenis data)
- `minAge` (TEXT): Batas usia minimum produk. (Penulis masih tidak yakin dengan jenis data)
- `relatedCategoryId` (TEXT): ID kategori terkait.
- `relatedCategoryName` (TEXT): Nama kategori terkait.
- `relatedCategoryUrl` (TEXT): URL kategori terkait.
- `transactionSuccess` (INTEGER): Jumlah transaksi sukses.
- `transactionReject` (INTEGER): Jumlah transaksi ditolak.
- `countSold` (INTEGER): Jumlah produk terjual.
- `paymentVerified` (INTEGER): Status pembayaran diverifikasi.
- `countView` (INTEGER): Jumlah tampilan produk.
- `countReview` (INTEGER): Jumlah ulasan produk.
- `countTalk` (INTEGER): Jumlah percakapan produk.
- `rating` (INTEGER): Rating produk.
- `productVariantsIdentifier` (TEXT): Identifier variasi produk.
- `productVariants` (TEXT): Variasi produk.

#### Tabel Shop Review List (`shop_review_list`)

- `shopId` (TEXT): ID unik toko.
- `reviewId` (TEXT): ID unik ulasan.
- `productName` (TEXT): Nama produk.
- `productPageUrl` (TEXT): URL halaman produk.
- `productStatus` (TEXT): Status produk.
- `isDeletedProduct` (INTEGER): Status produk dihapus. (0:FALSE,1:TRUE)
- `productVariantId` (TEXT): ID variasi produk.
- `productVariantName` (TEXT): Nama variasi produk.
- `rating` (INTEGER): Rating ulasan.
- `reviewTime` (TEXT): Waktu ulasan dibuat.
- `reviewText` (TEXT): Isi ulasan.
- `reviewerId` (TEXT): ID pelanggan.
- `reviewerName` (TEXT): Nama pelanggan.
- `avatar` (TEXT): URL avatar pelanggan.
- `replyText` (TEXT): Balasan dari toko.
- `replyTime` (TEXT): Waktu balasan.
- `attachmentId` (TEXT): ID lampiran.
- `thumbnailUrl` (TEXT): URL gambar thumbnail.
- `fullsizeUrl` (TEXT): URL gambar ukuran penuh.
- `isReportable` (INTEGER): Status dapat dilaporkan. (0:FALSE,1:TRUE)
- `isAnonymous` (INTEGER): Status anonim. (0:FALSE,1:TRUE)
- `totalLike` (INTEGER): Jumlah suka.
- `likeStatus` (INTEGER): Status suka.
- `badRatingReasonFmt` (TEXT): Alasan rating buruk.

