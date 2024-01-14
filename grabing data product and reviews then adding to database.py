# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 11:03:18 2024

@author: snsv
"""
import requests as req
import pandas as pd
import time
from urllib.parse import urlparse
import sqlite3
import os
from datetime import datetime
from sqlalchemy import create_engine, inspect

def get_tokopedia_shop_product(shopId:str)->pd.DataFrame:
    pages_count=1
    data_list = []
    url = 'https://gql.tokopedia.com/graphql/ShopProducts'
    header = {'Content-Type': 'application/json',
              'Accept': '*/*',
              'Accept-Encoding': 'gzip, deflate, br',
              'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
              'Content-Length': '3974',
              'Dnt': '1',
              'Origin': 'https://www.tokopedia.com',
              'Referer': f'https://www.tokopedia.com/lovebonitoid/product/page/{pages_count}',
              'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
              'Sec-Ch-Ua-Mobile': '?0',
              'Sec-Ch-Ua-Platform': '"Windows"',
              'Sec-Fetch-Dest': 'empty',
              'Sec-Fetch-Mode': 'cors',
              'Sec-Fetch-Site': 'same-site',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
              'X-Device': 'desktop-0.0',
              'X-Source': 'tokopedia-lite',
              'X-Tkpd-Lite-Service': 'zeus',
              'X-Version': 'a71fcbd'
             }
    payload = {"operationName": "ShopProducts",
               "query": "query ShopProducts($sid: String!, $source: String, $page: Int, $perPage: Int, $keyword: String, $etalaseId: String, $sort: Int, $user_districtId: String, $user_cityId: String, $user_lat: String, $user_long: String) { GetShopProduct(shopID: $sid, source: $source, filter: {page: $page, perPage: $perPage, fkeyword: $keyword, fmenu: $etalaseId, sort: $sort, user_districtId: $user_districtId, user_cityId: $user_cityId, user_lat: $user_lat, user_long: $user_long}) { status errors links { prev next __typename } data { name product_url product_id price { text_idr __typename } primary_image { original thumbnail resize300 __typename } flags { isSold isPreorder isWholesale isWishlist __typename } campaign { discounted_percentage original_price_fmt start_date end_date __typename } label { color_hex content __typename } label_groups { position title type url __typename } badge { title image_url __typename } stats { reviewCount rating averageRating __typename } category { id __typename } __typename } __typename } }",
               "variables": {"source": "shop",
                             "sid": shopId,
                             "page": pages_count,
                             "perPage": 80,
                             "etalaseId": "etalase",
                             "sort": 1,
                             "user_cityId": "",
                             "user_districtId": "",
                             "user_lat": "",
                             "user_long": ""
                            }
              }
    
    while True: 
        payload['variables']['page'] = pages_count
        retries = 0
        while retries < 11:
            try:
                request = req.post(url,headers=header,json=payload).json()
                if request is not None:
                    break
                else:
                    continue
            except (req.exceptions.ConnectionError) as e:
                print(f' error {e}')
                retries += 1
                time.sleep(3)
                print(f'try to sleep 3 second, tries count {retries}')
        #request = req.post(url,headers=header,json=payload).json()
        data = request['data']['GetShopProduct'].get('data')
        if len(data) <1:
            break
        else:
            for index in range(0,len(data),1):
                data_dict = {}
                data_dict['shopId'] = shopId
                data_dict['productListingStatus'] = 'True'
                data_dict['productName'] = data[index].get('name')
                data_dict['productUrl'] = data[index].get('product_url')
                data_dict['productId'] = data[index].get('product_id')
                if data[index].get('price') is not None:
                    data_dict['priceText'] = data[index]['price'].get('text_idr')
                else:
                    data_dict['priceText'] = None
                if data[index].get('campaign') is not None:
                    data_dict['discounted_percentage'] = data[index]['campaign'].get('discounted_percentage')
                    data_dict['original_price'] = data[index]['campaign'].get('original_price_fmt')
                    data_dict['start_date'] = data[index]['campaign'].get('start_date')
                    data_dict['end_date'] = data[index]['campaign'].get('end_date')
                else:
                    data_dict['discounted_percentage'] = None
                    data_dict['original_price'] = None
                    data_dict['start_date'] = None
                    data_dict['end_date'] = None    
                if data[index].get('primary_image') is not None:
                    data_dict['imageOriginal'] = data[index]['primary_image'].get('original')
                    data_dict['imageThumbnail'] = data[index]['primary_image'].get('thumbnail')
                    data_dict['imageResize300'] = data[index]['primary_image'].get('resize300')
                else:
                    data_dict['imageOriginal'] = None
                    data_dict['imageThumbnail'] = None
                    data_dict['imageResize300'] = None
                if data[index].get('flags') is not None:
                    data_dict['isSold'] = data[index]['flags'].get('isSold')
                    data_dict['isPreorder'] = data[index]['flags'].get('isPreorder')
                    data_dict['isWholesale'] = data[index]['flags'].get('isWholesale')
                else:
                    data_dict['isSold'] = None
                    data_dict['isPreorder'] = None
                    data_dict['isWholesale'] = None

                if data[index].get('badge') is not None:
                    data_dict['storeBadge'] = data[index]['badge'][0].get('title')
                else:
                    data_dict['storeBadge'] = None

                if data[index].get('stats') is not None:
                    data_dict['reviewCount'] = data[index]['stats'].get('reviewCount')
                    data_dict['rating'] = data[index]['stats'].get('rating')
                    data_dict['averageRating'] = data[index]['stats'].get('averageRating')
                else:
                    data_dict['reviewCount'] = None
                    data_dict['rating'] = None
                    data_dict['averageRating'] = None
                if data[index].get('category') is not None:
                    data_dict['Category'] = data[index]['category'].get('id')
                else:
                    data_dict['category'] = None
                if data_dict['productUrl'] is not None:
                    data_dict['shopDomain'] = (urlparse(data_dict['productUrl']).path).split('/')[1]
                    data_dict['productKey'] = (urlparse(data_dict['productUrl']).path).split('/')[2]
                    data_dict['extParam'] = (urlparse(data_dict['productUrl']).query).split('=')[1]
                else:
                    data_dict['shopDomain'] = None
                    data_dict['productKey'] = None
                    data_dict['extParam'] = None     
                data_list.append(data_dict)
            print(f'pages {pages_count} done')
            pages_count+=1
    print('finnish')
    return pd.DataFrame.from_records(data_list)


def get_tokopedia_basic_info(df:pd.DataFrame)->dict:
    date_now = datetime.now().date()
    url = 'https://gql.tokopedia.com/graphql/PDPGetLayoutQuery'
    header = {'Accept':'*/*',
              'Content-Type':'application/json',
              'Dnt':'1',
              'Referer': df['productUrl'],
              'Sec-Ch-Ua':'"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
              'Sec-Ch-Ua-Mobile':'?0',
              'Sec-Ch-Ua-Platform':'Windows',
              'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
              'X-Device':'desktop',
              'X-Source':'tokopedia-lite',
              'X-Tkpd-Akamai':'pdpGetLayout',
              'X-Tkpd-Lite-Service':'zeus',
              'X-Version':'fc1a9ee'
             }
    payload = {"operationName": "PDPGetLayoutQuery",
               "variables": {"shopDomain": df['shopDomain'],
                             "productKey": df['productKey'],
                             "layoutID": "",
                             "apiVersion": 1,
                             "tokonow": {"shopID":  df['shopId'],
                                         "whID": "0",
                                         "serviceType": "ooc"
                                        },
                             "deviceID": "",
                             "userLocation": {"cityID": "",
                                              "addressID": "",
                                              "districtID": "",
                                              "postalCode": "",
                                              "latlon": ""
                                             },
                             "extParam": df['extParam']
                            },
               "query": "fragment ProductVariant on pdpDataProductVariant {\n  errorCode\n  parentID\n  defaultChild\n  sizeChart\n  totalStockFmt\n  variants {\n    productVariantID\n    variantID\n    name\n    identifier\n    option {\n      picture {\n        urlOriginal: url\n        urlThumbnail: url100\n        __typename\n      }\n      productVariantOptionID\n      variantUnitValueID\n      value\n      hex\n      stock\n      __typename\n    }\n    __typename\n  }\n  children {\n    productID\n    price\n    priceFmt\n    slashPriceFmt\n    discPercentage\n    optionID\n    optionName\n    productName\n    productURL\n    picture {\n      urlOriginal: url\n      urlThumbnail: url100\n      __typename\n    }\n    stock {\n      stock\n      isBuyable\n      stockWordingHTML\n      minimumOrder\n      maximumOrder\n      __typename\n    }\n    isCOD\n    isWishlist\n    campaignInfo {\n      campaignID\n      campaignType\n      campaignTypeName\n      campaignIdentifier\n      background\n      discountPercentage\n      originalPrice\n      discountPrice\n      stock\n      stockSoldPercentage\n      startDate\n      endDate\n      endDateUnix\n      appLinks\n      isAppsOnly\n      isActive\n      hideGimmick\n      isCheckImei\n      minOrder\n      __typename\n    }\n    thematicCampaign {\n      additionalInfo\n      background\n      campaignName\n      icon\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ProductMedia on pdpDataProductMedia {\n  media {\n    type\n    urlOriginal: URLOriginal\n    urlThumbnail: URLThumbnail\n    urlMaxRes: URLMaxRes\n    videoUrl: videoURLAndroid\n    prefix\n    suffix\n    description\n    variantOptionID\n    __typename\n  }\n  videos {\n    source\n    url\n    __typename\n  }\n  __typename\n}\n\nfragment ProductCategoryCarousel on pdpDataCategoryCarousel {\n  linkText\n  titleCarousel\n  applink\n  list {\n    categoryID\n    icon\n    title\n    isApplink\n    applink\n    __typename\n  }\n  __typename\n}\n\nfragment ProductHighlight on pdpDataProductContent {\n  name\n  price {\n    value\n    currency\n    priceFmt\n    slashPriceFmt\n    discPercentage\n    __typename\n  }\n  campaign {\n    campaignID\n    campaignType\n    campaignTypeName\n    campaignIdentifier\n    background\n    percentageAmount\n    originalPrice\n    discountedPrice\n    originalStock\n    stock\n    stockSoldPercentage\n    threshold\n    startDate\n    endDate\n    endDateUnix\n    appLinks\n    isAppsOnly\n    isActive\n    hideGimmick\n    __typename\n  }\n  thematicCampaign {\n    additionalInfo\n    background\n    campaignName\n    icon\n    __typename\n  }\n  stock {\n    useStock\n    value\n    stockWording\n    __typename\n  }\n  variant {\n    isVariant\n    parentID\n    __typename\n  }\n  wholesale {\n    minQty\n    price {\n      value\n      currency\n      __typename\n    }\n    __typename\n  }\n  isCashback {\n    percentage\n    __typename\n  }\n  isTradeIn\n  isOS\n  isPowerMerchant\n  isWishlist\n  isCOD\n  preorder {\n    duration\n    timeUnit\n    isActive\n    preorderInDays\n    __typename\n  }\n  __typename\n}\n\nfragment ProductCustomInfo on pdpDataCustomInfo {\n  icon\n  title\n  isApplink\n  applink\n  separator\n  description\n  __typename\n}\n\nfragment ProductInfo on pdpDataProductInfo {\n  row\n  content {\n    title\n    subtitle\n    applink\n    __typename\n  }\n  __typename\n}\n\nfragment ProductDetail on pdpDataProductDetail {\n  content {\n    title\n    subtitle\n    applink\n    showAtFront\n    isAnnotation\n    __typename\n  }\n  __typename\n}\n\nfragment ProductDataInfo on pdpDataInfo {\n  icon\n  title\n  isApplink\n  applink\n  content {\n    icon\n    text\n    __typename\n  }\n  __typename\n}\n\nfragment ProductSocial on pdpDataSocialProof {\n  row\n  content {\n    icon\n    title\n    subtitle\n    applink\n    type\n    rating\n    __typename\n  }\n  __typename\n}\n\nfragment ProductDetailMediaComponent on pdpDataProductDetailMediaComponent {\n  title\n  description\n  contentMedia {\n    url\n    ratio\n    type\n    __typename\n  }\n  show\n  ctaText\n  __typename\n}\n\nquery PDPGetLayoutQuery($shopDomain: String, $productKey: String, $layoutID: String, $apiVersion: Float, $userLocation: pdpUserLocation, $extParam: String, $tokonow: pdpTokoNow, $deviceID: String) {\n  pdpGetLayout(shopDomain: $shopDomain, productKey: $productKey, layoutID: $layoutID, apiVersion: $apiVersion, userLocation: $userLocation, extParam: $extParam, tokonow: $tokonow, deviceID: $deviceID) {\n    requestID\n    name\n    pdpSession\n    basicInfo {\n      alias\n      createdAt\n      isQA\n      id: productID\n      shopID\n      shopName\n      minOrder\n      maxOrder\n      weight\n      weightUnit\n      condition\n      status\n      url\n      needPrescription\n      catalogID\n      isLeasing\n      isBlacklisted\n      isTokoNow\n      menu {\n        id\n        name\n        url\n        __typename\n      }\n      category {\n        id\n        name\n        title\n        breadcrumbURL\n        isAdult\n        isKyc\n        minAge\n        detail {\n          id\n          name\n          breadcrumbURL\n          isAdult\n          __typename\n        }\n        __typename\n      }\n      txStats {\n        transactionSuccess\n        transactionReject\n        countSold\n        paymentVerified\n        itemSoldFmt\n        __typename\n      }\n      stats {\n        countView\n        countReview\n        countTalk\n        rating\n        __typename\n      }\n      __typename\n    }\n    components {\n      name\n      type\n      position\n      data {\n        ...ProductMedia\n        ...ProductHighlight\n        ...ProductInfo\n        ...ProductDetail\n        ...ProductSocial\n        ...ProductDataInfo\n        ...ProductCustomInfo\n        ...ProductVariant\n        ...ProductCategoryCarousel\n        ...ProductDetailMediaComponent\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
              }
    retries = 0
    while retries < 4:
            try:
                request = req.post(url,headers=header,json=payload).json()
                if request is not None:
                    break
                else:
                    continue
            except (req.exceptions.ConnectionError) as e:
                print(f' error {e}')
                retries += 1
                time.sleep(1)
                print(f'try to sleep 10 second, tries count {retries}')
    data = request['data'].get('pdpGetLayout')
    data_dict = {}

    if data is not None:
        basicInfo = data.get('basicInfo')
        components = data.get('components')
        if basicInfo is not None:
            data_dict['alias'] = basicInfo.get('alias')
            data_dict['createdAt'] = basicInfo.get('createdAt')
            data_dict['dateRecorded'] = date_now
            data_dict['isQA'] = basicInfo.get('isQA')
            data_dict['productId'] = basicInfo.get('id')
            data_dict['shopID'] = basicInfo.get('shopID')
            data_dict['shopName'] = basicInfo.get('shopName')
            data_dict['minOrder'] = basicInfo.get('minOrder')
            data_dict['maxOrder'] = basicInfo.get('maxOrder')
            data_dict['weight'] = basicInfo.get('weight')
            data_dict['weightUnit'] = basicInfo.get('weightUnit')
            data_dict['condition'] = basicInfo.get('condition')
            data_dict['status'] = basicInfo.get('status')
            data_dict['productUrl'] = basicInfo.get('url')
            data_dict['needPrescription'] = basicInfo.get('needPrescription')
            data_dict['catalogID'] = basicInfo.get('catalogID')
            data_dict['isLeasing'] = basicInfo.get('isLeasing')
            data_dict['isBlacklisted'] = basicInfo.get('isBlacklisted')
            data_dict['isTokoNow'] = basicInfo.get('isTokoNow')
            if basicInfo.get('menu') is not None:
                data_dict['etalaseId'] = basicInfo['menu'].get('id')
                data_dict['etalaseName'] = basicInfo['menu'].get('name')
                data_dict['etalaseUrl'] = basicInfo['menu'].get('url')
            else:
                data_dict['etalaseId'] = None
                data_dict['etalaseName'] = None
                data_dict['etalaseUrl'] = None

            if basicInfo.get('category') is not None:
                data_dict['categoryId'] = basicInfo['menu'].get('id')
                data_dict['categoryName'] = basicInfo['menu'].get('name')
                data_dict['categoryTitle'] = basicInfo['menu'].get('title')
                data_dict['categoryUrl'] = basicInfo['menu'].get('breadcrumbURL')
                data_dict['isAdult'] = basicInfo['menu'].get('isAdult')
                data_dict['isKyc'] = basicInfo['menu'].get('isKyc')
                data_dict['minAge'] = basicInfo['menu'].get('minAge')
                if basicInfo['category'].get('detail') is not None:
                    temp_data = basicInfo['category'].get('detail')
                    temp_detail_list = []
                    for item in range(0,len(temp_data),1):
                        temp_detail_dict = {}
                        temp_detail_dict['categoryId'] = temp_data[item].get('id')
                        temp_detail_dict['categoryName'] = temp_data[item].get('name')
                        temp_detail_dict['categoryUrl'] = temp_data[item].get('breadcrumbURL')
                        temp_detail_list.append(temp_detail_dict)
                    data_dict['relatedCategory'] = str(temp_detail_list)
                else:
                    data_dict['relatedCategory'] = None


            else:
                data_dict['categoryId'] = None
                data_dict['categoryName'] = None
                data_dict['categoryTitle'] = None
                data_dict['categoryUrl'] = None
                data_dict['isAdult'] = None
                data_dict['isKyc'] = None
                data_dict['minAge'] = None
                data_dict['relatedCategory'] = None
            if basicInfo.get('txStats') is not None:
                data_dict['transactionSuccess'] = basicInfo['txStats'].get('transactionSuccess')
                data_dict['transactionReject'] = basicInfo['txStats'].get('transactionReject')
                data_dict['countSold'] = basicInfo['txStats'].get('countSold')
                data_dict['paymentVerified'] = basicInfo['txStats'].get('paymentVerified')
            else:
                data_dict['transactionSuccess'] = None
                data_dict['transactionReject'] = None
                data_dict['countSold'] = None
                data_dict['paymentVerified'] = None
            if basicInfo.get('stats') is not None:
                data_dict['countView'] = basicInfo['stats'].get('countView')
                data_dict['countReview'] = basicInfo['stats'].get('countReview')
                data_dict['countTalk'] = basicInfo['stats'].get('countTalk')
                data_dict['rating'] = basicInfo['stats'].get('rating')
            else:
                data_dict['countView'] = None
                data_dict['countReview'] = None
                data_dict['countTalk'] = None
                data_dict['rating'] = None
            if components is not None:
                for item in range(0,len(components),1):
                    if components[item].get('name') == 'new_variant_options':
                        if components[item].get('data') is not None:
                            if components[item]['data'][0].get('variants') is not None:
                                identifier = components[item]['data'][0]['variants'][0].get('identifier')
                                if identifier is not None:
                                    identifier_dict = {}
                                    if components[item]['data'][0]['variants'][0].get('option') is not None:
                                        variants_list = []
                                        for var in range(0,len(components[item]['data'][0]['variants'][0].get('option')),1):
                                            variants_list.append(components[item]['data'][0]['variants'][0]['option'][var].get('value'))
                                        identifier_dict[identifier] = variants_list
                                        
                                        data_dict['productVariants'] = str(identifier_dict)
                                    else:
                                        data_dict['productVariants'] = None    
                                else:
                                    data_dict['productVariants'] = None
                            else:
                                data_dict['productVariants'] = None

                        else:
                            data_dict['productVariants'] = None
                    else:
                        pass
            return data_dict
        else:
            pass
    else:
        return None


def get_review_data()->pd.DataFrame:
    data_list = []
    pages = 1
    while True:

        url = 'https://gql.tokopedia.com/graphql/ReviewList'
        header = {'Accept':'*/*',
                  'Content-Type':'application/json',
                  'Dnt':'1',
                  'Referer':'https://www.tokopedia.com/lovebonitoid/review',
                  'Sec-Ch-Ua':'"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                  'Sec-Ch-Ua-Mobile':'?0',
                  'Sec-Ch-Ua-Platform':'"Windows"',
                  'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                  'X-Source':'tokopedia-lite',
                  'X-Tkpd-Lite-Service':'zeus',
                  'X-Version':'6bad28a'
                 }
        payload = {"operationName": "ReviewList",
                   "variables": {"shopID": "9633142",
                                 "page": pages,
                                 "limit": 100,
                                 "sortBy": "create_time desc",
                                 "filterBy": ""
                                },
                   "query": "query ReviewList($shopID: String!, $limit: Int!, $page: Int!, $filterBy: String, $sortBy: String) {\n  productrevGetShopReviewReadingList(shopID: $shopID, limit: $limit, page: $page, filterBy: $filterBy, sortBy: $sortBy) {\n    list {\n      id: reviewID\n      product {\n        productID\n        productName\n        productImageURL\n        productPageURL\n        productStatus\n        isDeletedProduct\n        productVariant {\n          variantID\n          variantName\n          __typename\n        }\n        __typename\n      }\n      rating\n      reviewTime\n      reviewText\n      reviewerID\n      reviewerName\n      avatar\n      replyText\n      replyTime\n      attachments {\n        attachmentID\n        thumbnailURL\n        fullsizeURL\n        __typename\n      }\n      videoAttachments {\n        attachmentID\n        videoUrl\n        __typename\n      }\n      state {\n        isReportable\n        isAnonymous\n        __typename\n      }\n      likeDislike {\n        totalLike\n        likeStatus\n        __typename\n      }\n      badRatingReasonFmt\n      __typename\n    }\n    hasNext\n    shopName\n    totalReviews\n    __typename\n  }\n}\n"
                  }
        retries = 0
        while retries < 4:
                try:
                    request = req.post(url,headers=header,json=payload).json()
                    if request is not None:
                        break
                    else:
                        continue
                except (req.exceptions.ConnectionError) as e:
                    print(f' error {e}')
                    retries += 1
                    time.sleep(1)
                    print(f'try to sleep 10 second, tries count {retries}')

        temp_cursor = request['data']['productrevGetShopReviewReadingList']['list']
        next_pages = request['data']['productrevGetShopReviewReadingList'].get('hasNext')
        for lenght in range(0,len(temp_cursor),1):   
            dict_review_dict = {}
            dict_review_dict['id'] = temp_cursor[lenght].get('id')
            if temp_cursor[lenght].get('product') is not None:
                dict_review_dict['productName'] = temp_cursor[lenght]['product'].get('productName')
                dict_review_dict['productPageURL'] = temp_cursor[lenght]['product'].get('productPageURL')
                dict_review_dict['productStatus'] = temp_cursor[lenght]['product'].get('productStatus')
                dict_review_dict['isDeletedProduct'] = temp_cursor[lenght]['product'].get('isDeletedProduct')
                dict_review_dict['productPageURL'] = temp_cursor[lenght]['product'].get('productPageURL')
                if temp_cursor[lenght]['product'].get('productVariant') is not None:
                    dict_review_dict['productVariantID'] = temp_cursor[lenght]['product'].get('productPageURL')
                    dict_review_dict['productVariantName'] = temp_cursor[lenght]['product'].get('productPageURL')
                else:
                    dict_review_dict['productVariantID'] = None
                    dict_review_dict['productVariantName'] = None
            else:
                dict_review_dict['productName'] = None
                dict_review_dict['productPageURL'] = None
                dict_review_dict['productStatus'] = None
                dict_review_dict['isDeletedProduct'] = None
                dict_review_dict['productPageURL'] = None
                dict_review_dict['productVariantID'] = None
                dict_review_dict['productVariantName'] = None
            dict_review_dict['rating'] = temp_cursor[lenght].get('rating')
            dict_review_dict['reviewTime'] = temp_cursor[lenght].get('reviewTime')
            dict_review_dict['reviewText'] = temp_cursor[lenght].get('reviewText')
            dict_review_dict['reviewerID'] = temp_cursor[lenght].get('reviewerID')
            dict_review_dict['reviewerName'] = temp_cursor[lenght].get('reviewerName')
            dict_review_dict['avatar'] = temp_cursor[lenght].get('avatar')
            dict_review_dict['replyText'] = temp_cursor[lenght].get('replyText')
            dict_review_dict['replyTime'] = temp_cursor[lenght].get('replyTime')
            if temp_cursor[lenght].get('attachments') is not None:
                list_id = []
                list_thumbnail = []
                list_fullsize = []
                for item in range(0,len(temp_cursor[lenght].get('attachments')),1):
                    list_id.append(temp_cursor[lenght]['attachments'][item].get('attachmentID'))
                    list_fullsize.append(temp_cursor[lenght]['attachments'][item].get('fullsizeURL'))
                    list_thumbnail.append(temp_cursor[lenght]['attachments'][item].get('thumbnailURL'))
                dict_review_dict['attachmentID'] = str(list_id)
                dict_review_dict['thumbnailURL'] = str(list_thumbnail)
                dict_review_dict['fullsizeURL'] = str(list_fullsize)
            else:
                dict_review_dict['attachmentID'] = None
                dict_review_dict['thumbnailURL'] = None
                dict_review_dict['fullsizeURL'] = None
            if temp_cursor[lenght].get('state') is not None:    
                dict_review_dict['isReportable'] = temp_cursor[lenght]['state'].get('isReportable')
                dict_review_dict['isAnonymous'] = temp_cursor[lenght]['state'].get('isAnonymous')
            else:
                dict_review_dict['isReportable'] = None
                dict_review_dict['isAnonymous'] = None
            if temp_cursor[lenght].get('likeDislike') is not None:
                dict_review_dict['totalLike'] = temp_cursor[lenght]['likeDislike'].get('totalLike')
                dict_review_dict['likeStatus'] = temp_cursor[lenght]['likeDislike'].get('likeStatus')
            else:
                dict_review_dict['totalLike'] = None
                dict_review_dict['likeStatus'] = None
            dict_review_dict['badRatingReasonFmt'] = temp_cursor[lenght].get('badRatingReasonFmt')
            data_list.append(dict_review_dict)
        if next_pages == False:
            break
        else:
            pages += 1
    return pd.DataFrame.from_records(data_list)

def main():
    file_path = "love bonito.db"
    shop_id = "9633142"
    active_product_table = 'active_product'
    product_basic_info_table = 'productBasicInfo'
    review_list_table = 'total review'
    date_now = datetime.now().date()
    date_now = pd.to_datetime(date_now)
    if not os.path.exists(file_path):    
        with sqlite3.connect(file_path) as connection:
            pass
    else:
        sql_engine = create_engine(f'sqlite:///{file_path}')
        # Create an Inspector
        inspector = inspect(sql_engine)
    
        # Check if the "my_table" table exists
        #data_dict['productListingStatus'] = 'True'
        df = get_tokopedia_shop_product(shop_id)
        if inspector.has_table(active_product_table):
            query = f"SELECT * FROM {active_product_table} WHERE shopId = '{shop_id}'"
            df_existing = pd.read_sql_query(query, sql_engine)
            # Update table values where df_existing['productId'] matches df['productId']
            mask = df_existing['productId'].isin(df['productId'])
            
            if len(df_existing.loc[~mask, 'productListingStatus'])>0:
                df_existing.loc[~mask, 'productListingStatus'] = False
            elif len(df_existing.loc[mask])>0:
                df_existing.loc[mask] = df[df['productId'].isin(df_existing['productId'])]
            elif len(df[~df['productId'].isin(df_existing['productId'])])>0:
                df_new_rows = df[~df['productId'].isin(df_existing['productId'])]
                df_existing = pd.concat([df_existing, df_new_rows], ignore_index=True)
            else:
                pass
            # Add new rows where df_existing['productId'] not matches df['productId']
            
            
            # Save the updated DataFrame back to the database
            df_existing.to_sql(active_product_table, con=sql_engine, index=False, if_exists='replace')
            #del df_existing, mask, df_new_rows
            # delete variable df_existing
            if inspector.has_table(product_basic_info_table):
                print(f'table {product_basic_info_table} exist')
                print('update data')
                temp_df_sql = pd.DataFrame()
                for index in range(0,len(df),1):
                    #product_id = df['productId'].iloc[index]
                    query = f"SELECT * FROM {product_basic_info_table} WHERE productId == {df['productId'].iloc[index]}"
                    df_existing_basicInfo_table = pd.read_sql_query(query, sql_engine)
                    df_existing_basicInfo_table['dateRecorded'] = pd.to_datetime(df_existing_basicInfo_table['dateRecorded'])
                    #dict_basic_info_data['productVariants'] = dict_basic_info_data['productVariants'].json()
                    #dict_basic_info_data['productVariants'] = json.dumps(dict_basic_info_data['productVariants'])
                    
                    #dict_basic_info_data['productVariants'] = json.dumps(dict_basic_info_data['productVariants'])
                    if (len(df_existing_basicInfo_table) == 1) and (df_existing_basicInfo_table['dateRecorded'].iloc[0] < date_now):
                        dict_basic_info_data = get_tokopedia_basic_info(df.iloc[index])
                        dict_basic_info_data = pd.DataFrame([dict_basic_info_data])
                        dict_basic_info_data['dateRecorded'] = pd.to_datetime(dict_basic_info_data['dateRecorded'])
                        dict_basic_info_data.drop_duplicates(subset='productId', keep='first', inplace=True, ignore_index=False)
                        temp_df_sql = pd.concat([temp_df_sql, dict_basic_info_data], ignore_index=True)
                        print(f'update data product id: {dict_basic_info_data["productId"].iloc[0]}; {index}/{len(df)}')
                        
    
                            
                    elif (len(df_existing_basicInfo_table) == 0):
                        dict_basic_info_data = get_tokopedia_basic_info(df.iloc[index])
                        dict_basic_info_data = pd.DataFrame([dict_basic_info_data])
                        dict_basic_info_data['dateRecorded'] = pd.to_datetime(dict_basic_info_data['dateRecorded'])
                        dict_basic_info_data.drop_duplicates(subset='productId', keep='first', inplace=True, ignore_index=False)
                        dict_basic_info_data.to_sql(product_basic_info_table, con=sql_engine, index=False, if_exists='append')
                        print(f'add new data product id: {dict_basic_info_data["productId"].iloc[0]}; {index}/{len(df)}')
                        
                    else:
                        print(f'data up to date, check next data; {index}/{len(df)}')
                query = f"SELECT * FROM {product_basic_info_table}"
                df_existing_basicInfo_table = pd.read_sql_query(query, sql_engine)
                df_existing_basicInfo_table.update(temp_df_sql)
                df_existing_basicInfo_table.to_sql(product_basic_info_table, con=sql_engine, index=False, if_exists='replace')
                reviewData = get_review_data()
                print('update list review table')
                reviewData.to_sql(review_list_table, con=sql_engine, index=False, if_exists='replace')
    
            else:
                print(f'table {product_basic_info_table} not exist')
                print('grabbing new data')   
                dict_basic_info_data = get_tokopedia_basic_info(df.iloc[0])
                dict_basic_info_data = pd.DataFrame([dict_basic_info_data])
                dict_basic_info_data['dateRecorded'] = pd.to_datetime(dict_basic_info_data['dateRecorded'])
                #dict_basic_info_data['productVariants'] = dict_basic_info_data['productVariants'].json()
    
                dict_basic_info_data.drop_duplicates(subset='productId', keep='first', inplace=True, ignore_index=False)
                #dict_basic_info_data['productVariants'] = json.dumps(dict_basic_info_data['productVariants'])
    
                dict_basic_info_data.to_sql(product_basic_info_table, con=sql_engine, index=False, if_exists='replace')
                for index in range(1,len(df),1):
                    print(f'grabbing data product id: {df["productId"].iloc[index]}')
                    dict_basic_info_data = get_tokopedia_basic_info(df.iloc[index])
                    dict_basic_info_data = pd.DataFrame([dict_basic_info_data])
                    dict_basic_info_data['dateRecorded'] = pd.to_datetime(dict_basic_info_data['dateRecorded'])
                    #dict_basic_info_data['productVariants'] = dict_basic_info_data['productVariants'].json()
    
                    dict_basic_info_data.drop_duplicates(subset='productId', keep='first', inplace=True, ignore_index=False)
                    #dict_basic_info_data['productVariants'] = json.dumps(dict_basic_info_data['productVariants'])
    
                    dict_basic_info_data.to_sql(product_basic_info_table, con=sql_engine, index=False, if_exists='append')
                reviewData = get_review_data()
                print('update list review table')
                reviewData.to_sql(review_list_table, con=sql_engine, index=False, if_exists='replace')
    
        else:
            df.to_sql(active_product_table, con=sql_engine, index=False)
            print('grabbing new data')   
            dict_basic_info_data = get_tokopedia_basic_info(df.iloc[0])
            dict_basic_info_data = pd.DataFrame([dict_basic_info_data])
            dict_basic_info_data['dateRecorded'] = pd.to_datetime(dict_basic_info_data['dateRecorded'])
            #dict_basic_info_data['productVariants'] = dict_basic_info_data['productVariants'].json()
    
            dict_basic_info_data.drop_duplicates(subset='productId', keep='first', inplace=True, ignore_index=False)
            #dict_basic_info_data['productVariants'] = json.dumps(dict_basic_info_data['productVariants'])
    
            dict_basic_info_data.to_sql(product_basic_info_table, con=sql_engine, index=False, if_exists='replace')
            for index in range(1,len(df),1):
                print(f'grabbing data product id: {df["productId"].iloc[index]}')
                dict_basic_info_data = get_tokopedia_basic_info(df.iloc[index])
                dict_basic_info_data = pd.DataFrame([dict_basic_info_data])
                dict_basic_info_data['dateRecorded'] = pd.to_datetime(dict_basic_info_data['dateRecorded'])
                #dict_basic_info_data['productVariants'] = dict_basic_info_data['productVariants'].json()
    
                dict_basic_info_data.drop_duplicates(subset='productId', keep='first', inplace=True, ignore_index=False)
                #dict_basic_info_data['productVariants'] = json.dumps(dict_basic_info_data['productVariants'])
                dict_basic_info_data.to_sql(product_basic_info_table, con=sql_engine, index=False, if_exists='append')
            reviewData = get_review_data()
            print('update list review table')
            reviewData.to_sql(review_list_table, con=sql_engine, index=False, if_exists='replace')


if __name__ == "__main__":
    main()