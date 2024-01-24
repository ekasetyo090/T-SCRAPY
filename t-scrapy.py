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
from sqlalchemy import create_engine


def get_tokopedia_shop_info(shop_url:str)->pd.DataFrame:
    domain = ((urlparse(shop_url)).path).replace('/','')
    date_now = datetime.now().date()
    url = 'https://gql.tokopedia.com/graphql/ShopInfoCore'
    header = {'Referer': shop_url,
              'Sec-Ch-Ua':'"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
              'Sec-Ch-Ua-Mobile':'?0',
              'Sec-Ch-Ua-Platform':'"Windows"',
              'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
              'X-Source':'tokopedia-lite',
              'X-Tkpd-Lite-Service':'zeus',
              'X-Version':'e67d65f'}
    payload = {"operationName": "ShopInfoCore",
               "variables": {"id": 0,
                             "domain": domain
                            },
               "query": "query ShopInfoCore($id: Int!, $domain: String) {\n  shopInfoByID(input: {shopIDs: [$id], fields: [\"active_product\", \"allow_manage_all\", \"assets\", \"core\", \"closed_info\", \"create_info\", \"favorite\", \"location\", \"status\", \"is_open\", \"other-goldos\", \"shipment\", \"shopstats\", \"shop-snippet\", \"other-shiploc\", \"shopHomeType\", \"branch-link\", \"goapotik\", \"fs_type\"], domain: $domain, source: \"shoppage\"}) {\n    result {\n      shopCore {\n        description\n        domain\n        shopID\n        name\n        tagLine\n        defaultSort\n        __typename\n      }\n      createInfo {\n        openSince\n        __typename\n      }\n      favoriteData {\n        totalFavorite\n        alreadyFavorited\n        __typename\n      }\n      activeProduct\n      shopAssets {\n        avatar\n        cover\n        __typename\n      }\n      location\n      isAllowManage\n      branchLinkDomain\n      isOpen\n      shipmentInfo {\n        isAvailable\n        image\n        name\n        product {\n          isAvailable\n          productName\n          uiHidden\n          __typename\n        }\n        __typename\n      }\n      shippingLoc {\n        districtName\n        cityName\n        __typename\n      }\n      shopStats {\n        productSold\n        totalTxSuccess\n        totalShowcase\n        __typename\n      }\n      statusInfo {\n        shopStatus\n        statusMessage\n        statusTitle\n        tickerType\n        __typename\n      }\n      closedInfo {\n        closedNote\n        until\n        reason\n        detail {\n          status\n          __typename\n        }\n        __typename\n      }\n      bbInfo {\n        bbName\n        bbDesc\n        bbNameEN\n        bbDescEN\n        __typename\n      }\n      goldOS {\n        isGold\n        isGoldBadge\n        isOfficial\n        badge\n        shopTier\n        __typename\n      }\n      shopSnippetURL\n      customSEO {\n        title\n        description\n        bottomContent\n        __typename\n      }\n      isQA\n      isGoApotik\n      partnerInfo {\n        fsType\n        __typename\n      }\n      __typename\n    }\n    error {\n      message\n      __typename\n    }\n    __typename\n  }\n}\n"
              }
    retries = 0
    while retries < 4:
            try:
                request = req.post(url,headers=header,json=payload).json()
                if (request.get('data') is not None) and (request['data'].get('shopInfoByID') is not None) and (request['data']['shopInfoByID'].get('result') is not None):
                    request = request['data']['shopInfoByID']['result'][0]
                    break
                else:
                    continue
            except (req.exceptions.ConnectionError):
                retries += 1
                time.sleep(1)
    temp_data = {}
    ####################################
    if request.get('shopCore') is not None:
        temp_data['description'] = request['shopCore'].get('description')
        temp_data['domain'] = request['shopCore'].get('domain')
        temp_data['shopId'] = request['shopCore'].get('shopID')
        temp_data['name'] = request['shopCore'].get('name')
        temp_data['nameSearch'] = temp_data['name'].lower()
        temp_data['tagLine'] = request['shopCore'].get('tagLine')
        temp_data['defaultSort'] = request['shopCore'].get('defaultSort')
    elif request.get('shopCore') is None:
        temp_data['description'] = None
        temp_data['domain'] = None
        temp_data['shopId'] = None
        temp_data['name'] = None
        temp_data['nameSearch'] = None
        temp_data['tagLine'] = None
        temp_data['defaultSort'] = None
    temp_data['dateRecorded'] = date_now
    ####################################
    if request.get('createInfo') is not None:
        temp_data['dateOpen'] = request['createInfo'].get('openSince')
    elif request.get('createInfo') is None:
        temp_data['dateOpen'] = None
    ####################################
    if request.get('favoriteData') is not None:
        temp_data['totalFavorite'] = request['favoriteData'].get('totalFavorite')
        temp_data['alreadyFavorited'] = request['favoriteData'].get('alreadyFavorited')
    elif request.get('favoriteData') is None:
        temp_data['totalFavorite'] = None
        temp_data['alreadyFavorited'] = None
    ####################################
    temp_data['activeProduct'] = request.get('activeProduct')
    ####################################
    if request.get('shopAssets') is not None:
        temp_data['avatar'] = request['shopAssets'].get('avatar')
        temp_data['cover'] = request['shopAssets'].get('cover')
    elif request.get('shopAssets') is None:
        temp_data['avatar'] = None
        temp_data['cover'] = None
    ####################################
    temp_data['location'] = request.get('location')
    temp_data['isAllowManage'] = request.get('isAllowManage')
    temp_data['branchLinkDomain'] = request.get('branchLinkDomain')
    temp_data['isOpen'] = request.get('isOpen')
    ####################################
    if request.get('shipmentInfo') is not None:
        shipmentInfo_str = str()
        for i in range(len(request.get('shipmentInfo'))):
            data_holder = request['shipmentInfo'][i].get('name')
            shipmentInfo_str = shipmentInfo_str+data_holder+'||'
        temp_data['shipmentOption'] = shipmentInfo_str
    elif request.get('shipmentInfo') is None:
        temp_data['shipmentOption'] = None
    ####################################
    if request.get('shippingLoc') is not None:
        temp_data['shippingDistrict'] = request['shippingLoc'].get('districtName')
        temp_data['shippingCity'] = request['shippingLoc'].get('cityName')
    elif request.get('shippingLoc') is None:
        temp_data['shippingDistrict'] = None
        temp_data['shippingCity'] = None
    ####################################
    if request.get('shopStats') is not None:
        temp_data['productSold'] = request['shopStats'].get('productSold')
        temp_data['totalTxSuccess'] = request['shopStats'].get('totalTxSuccess')
        temp_data['totalShowcase'] = request['shopStats'].get('totalShowcase')
    elif request.get('shopStats') is None:
        temp_data['productSold'] = None
        temp_data['totalTxSuccess'] = None
        temp_data['totalShowcase'] = None
    ####################################
    if request.get('closedInfo') is not None:
        temp_data['closedNote'] = request['closedInfo'].get('closedNote')
        temp_data['closedUntil'] = request['closedInfo'].get('until')
        temp_data['closedReason'] = request['closedInfo'].get('reason')
    elif request.get('closedInfo') is None:
        temp_data['closedNote'] = None
        temp_data['closedUntil'] = None
        temp_data['closedReason'] = None
    ####################################
    if request.get('goldOS') is not None:
        temp_data['isGoldShop'] = request['goldOS'].get('isGold')
        temp_data['isGoldBadgeShop'] = request['goldOS'].get('isGoldBadge')
        temp_data['isOfficialShop'] = request['goldOS'].get('isOfficial')
    elif request.get('goldOS') is None:
        temp_data['isGoldShop'] = None
        temp_data['isGoldBadgeShop'] = None
        temp_data['isOfficialShop'] = None
    ####################################
    temp_data['shopSnippetURL'] = request.get('shopSnippetURL')
    ####################################
    if request.get('customSEO') is not None:
        temp_data['titleSEO'] = request['customSEO'].get('title')
        temp_data['descriptionSEO'] = request['customSEO'].get('description')
        temp_data['bottomContentSEO'] = request['customSEO'].get('bottomContent')
    elif request.get('customSEO') is None:
        temp_data['titleSEO'] = None
        temp_data['descriptionSEO'] = None
        temp_data['bottomContentSEO'] = None
    temp_data['isQA'] = request.get('isQA')
    temp_data['isGoApotik'] = request.get('isGoApotik')

    for key in temp_data.keys():
        if temp_data[key] == "":
            temp_data[key] = None
        else:
            pass

    df = pd.DataFrame([temp_data])
    df['dateOpen'] = pd.to_datetime(df['dateOpen'], format="%B %Y")
    df['dateRecorded'] = pd.to_datetime(df['dateRecorded'])
    for key in df.columns:
        if df[key].dtype == bool:
            df[key] = df[key].apply(lambda x: 1 if x else 0)
    for item in ['isQA','isGoApotik','isOfficialShop','isGoldBadgeShop','isGoldShop','totalShowcase','totalTxSuccess','productSold','isOpen','isAllowManage','activeProduct','alreadyFavorited','totalFavorite','defaultSort']:
        df[item] = pd.to_numeric(df[item],downcast="integer")
    return df
################################################################
def price_parse(price:str)->str:
    price = price[2:]
    price = price.replace('.','')
    return price
######################################################
def get_tokopedia_shop_product(shopId:str)->pd.DataFrame:
    date_now = datetime.now().date()
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
                if (request is not None) and (request.get('data') is not None) and (request['data'].get('GetShopProduct') is not None) and (request['data']['GetShopProduct'].get('data') is not None):
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
                data_dict['dateRecorded'] = date_now
                data_dict['shopId'] = shopId
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
                    data_dict['category'] = data[index]['category'].get('id')
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
                for key in data_dict.keys():
                    if data_dict[key] == "":
                        data_dict[key] = None
                    else:
                        pass
                data_list.append(data_dict)
            print(f'get product list pages {pages_count} done')
            pages_count+=1
    print('get product list done')
    data_list = pd.DataFrame.from_records(data_list)
    data_list['averageRating'] = pd.to_numeric(data_list['averageRating'], downcast="float") 
    for item in ['start_date','end_date','dateRecorded']:
        data_list[item] = pd.to_datetime(data_list[item])
    for item in ['rating','reviewCount','isWholesale','isPreorder','isSold','discounted_percentage']:
        data_list[item] = pd.to_numeric(data_list[item],downcast="integer")
    data_list['priceText'] = data_list['priceText'].apply(lambda x: pd.to_numeric(price_parse(x)) if x is not None else None)
    data_list['original_price'] = data_list['original_price'].apply(lambda x: pd.to_numeric(price_parse(x)) if x is not None else None)


    for key in data_list.columns:
        if data_list[key].dtype == bool:
            data_list[key] = data_list[key].apply(lambda x: 1 if x else 0)

    return data_list
################################################################

def get_tokopedia_shop_product_basic_info(df:pd.DataFrame)->pd.DataFrame:
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
                if (request is not None) and (request.get('data') is not None) and (request['data'].get('pdpGetLayout') is not None) and (request['data']['pdpGetLayout'].get('basicInfo') is not None):
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
            data_dict['shopId'] = basicInfo.get('shopID')
            data_dict['shopName'] = basicInfo.get('shopName')
            data_dict['minOrder'] = basicInfo.get('minOrder')
            data_dict['maxOrder'] = basicInfo.get('maxOrder')
            data_dict['weight'] = basicInfo.get('weight')
            data_dict['weightUnit'] = basicInfo.get('weightUnit')
            data_dict['condition'] = basicInfo.get('condition')
            data_dict['status'] = basicInfo.get('status')
            data_dict['productUrl'] = basicInfo.get('url')
            data_dict['isNeedPrescription'] = basicInfo.get('needPrescription')
            data_dict['catalogId'] = basicInfo.get('catalogID')
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
                    temp_detail_list_id = str()
                    temp_detail_list_name = str()
                    temp_detail_list_url = str()
                    for item in range(0,len(temp_data),1):
                        temp_detail_list_id = temp_detail_list_id+((temp_data[item].get('id'))+'||')
                        temp_detail_list_name = temp_detail_list_name+((temp_data[item].get('name'))+'||')
                        temp_detail_list_url = temp_detail_list_url+((temp_data[item].get('breadcrumbURL'))+'||')
                    data_dict['relatedCategoryId'] = temp_detail_list_id
                    data_dict['relatedCategoryName'] = temp_detail_list_name
                    data_dict['relatedCategoryUrl'] = temp_detail_list_url
                else:
                    data_dict['relatedCategoryId'] = None
                    data_dict['relatedCategoryName'] = None
                    data_dict['relatedCategoryUrl'] = None


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
                                #data_dict['productVariantsIdentifier'] = components[item]['data'][0]['variants'][0].get('identifier')
                                identifier = components[item]['data'][0]['variants'][0].get('identifier')
                                if identifier is not None:
                                    if components[item]['data'][0]['variants'][0].get('option') is not None:
                                        variants_list = str()
                                        for var in range(0,len(components[item]['data'][0]['variants'][0].get('option')),1):
                                            variants_list = variants_list+(components[item]['data'][0]['variants'][0]['option'][var].get('value'))+'||'
                                            #variants_list.append(components[item]['data'][0]['variants'][0]['option'][var].get('value'))
                                        data_dict['productVariantsIdentifier'] = identifier
                                        data_dict['productVariants'] = variants_list
                                    else:
                                        data_dict['productVariantsIdentifier'] = None
                                        data_dict['productVariants'] = None
                                else:
                                    data_dict['productVariantsIdentifier'] = None
                                    data_dict['productVariants'] = None
                            else:
                                data_dict['productVariantsIdentifier'] = None
                                data_dict['productVariants'] = None

                        else:
                            data_dict['productVariantsIdentifier'] = None
                            data_dict['productVariants'] = None
                    elif (item < (len(components)-1)) and components[item].get('name') != 'new_variant_options':
                        pass
                    elif (item == (len(components)-1)) and components[item].get('name') != 'new_variant_options':
                        data_dict['productVariantsIdentifier'] = None
                        data_dict['productVariants'] = None
            elif components is None:
                data_dict['productVariantsIdentifier'] = None
                data_dict['productVariants'] = None
            for key in data_dict.keys():
                if data_dict[key] == "":
                    data_dict[key] = None
            data_dict = pd.DataFrame([data_dict])
            data_dict['createdAt'] = pd.to_datetime(data_dict['createdAt'])
            data_dict['dateRecorded'] = pd.to_datetime(data_dict['dateRecorded'])
            
            for key in data_dict.columns:
                if data_dict[key].dtype == bool:
                    data_dict[key] = data_dict[key].apply(lambda x: 1 if x else 0)
            for item in ['rating','countTalk','countReview','countView','paymentVerified','countSold','transactionReject','transactionSuccess','isTokoNow','isBlacklisted','isLeasing','isNeedPrescription','weight','maxOrder','minOrder','isQA']:
                data_dict[item] = pd.to_numeric(data_dict[item],downcast="integer")
            return data_dict
        else:
            return None
    else:
        return None

def get_review_data(url:str,shopId:str)->pd.DataFrame:
    data_list = []
    pages = 1
    while True:

        url = 'https://gql.tokopedia.com/graphql/ReviewList'
        header = {'Accept':'*/*',
                  'Content-Type':'application/json',
                  'Dnt':'1',
                  'Referer': url+'/review',
                  'Sec-Ch-Ua':'"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                  'Sec-Ch-Ua-Mobile':'?0',
                  'Sec-Ch-Ua-Platform':'"Windows"',
                  'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                  'X-Source':'tokopedia-lite',
                  'X-Tkpd-Lite-Service':'zeus',
                  'X-Version':'6bad28a'
                 }
        payload = {"operationName": "ReviewList",
                   "variables": {"shopID": shopId,
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
                    if (request is not None) and (request.get('data') is not None) and (request['data'].get('productrevGetShopReviewReadingList') is not None):
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
            dict_review_dict['shopId'] = shopId
            dict_review_dict['reviewId'] = temp_cursor[lenght].get('id')
            if temp_cursor[lenght].get('product') is not None:
                dict_review_dict['productName'] = temp_cursor[lenght]['product'].get('productName')
                dict_review_dict['productPageUrl'] = temp_cursor[lenght]['product'].get('productPageURL')
                dict_review_dict['productStatus'] = temp_cursor[lenght]['product'].get('productStatus')
                dict_review_dict['isDeletedProduct'] = temp_cursor[lenght]['product'].get('isDeletedProduct')
                dict_review_dict['productPageUrl'] = temp_cursor[lenght]['product'].get('productPageURL')
                if temp_cursor[lenght]['product'].get('productVariant') is not None:
                    dict_review_dict['productVariantId'] = temp_cursor[lenght]['product'].get('productPageURL')
                    dict_review_dict['productVariantName'] = temp_cursor[lenght]['product'].get('productPageURL')
                else:
                    dict_review_dict['productVariantId'] = None
                    dict_review_dict['productVariantName'] = None
            else:
                dict_review_dict['productName'] = None
                dict_review_dict['productPageUrl'] = None
                dict_review_dict['productStatus'] = None
                dict_review_dict['isDeletedProduct'] = None
                dict_review_dict['productPageUrl'] = None
                dict_review_dict['productVariantId'] = None
                dict_review_dict['productVariantName'] = None
            dict_review_dict['rating'] = temp_cursor[lenght].get('rating')
            dict_review_dict['reviewTime'] = temp_cursor[lenght].get('reviewTime')
            dict_review_dict['reviewText'] = temp_cursor[lenght].get('reviewText')
            dict_review_dict['reviewerId'] = temp_cursor[lenght].get('reviewerID')
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
                dict_review_dict['attachmentId'] = str(list_id)
                dict_review_dict['thumbnailUrl'] = str(list_thumbnail)
                dict_review_dict['fullsizeUrl'] = str(list_fullsize)
            else:
                dict_review_dict['attachmentId'] = None
                dict_review_dict['thumbnailUrl'] = None
                dict_review_dict['fullsizeUrl'] = None
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
            for key in dict_review_dict.keys():
                if dict_review_dict[key] == "":
                    dict_review_dict[key] = None
            for key in dict_review_dict.keys():
                if dict_review_dict[key] == "[]":
                    dict_review_dict[key] = None
            data_list.append(dict_review_dict)
        print(f'get review pages {pages}')
        if next_pages == False:
            break
        else:
            pages += 1
    data_list = pd.DataFrame.from_records(data_list)
    for i in ['likeStatus','totalLike','isAnonymous','isReportable','rating','isDeletedProduct','productStatus']:
        data_list[i] = pd.to_numeric(data_list[i],downcast="integer")
    for key in data_list.columns:
        if data_list[key].dtype == bool:
            data_list[key] = data_list[key].apply(lambda x: 1 if x else 0)
    return data_list

def main():
    while True:
        try:
            shop_url_input = input("Enter tokopedia shop url:")
            if not shop_url_input.startswith("https://www.tokopedia.com/"):
                raise ValueError("Not a valid URL")
            else:
                break
        except ValueError as e:
            print(e)
        else:
            print(shop_url_input)
    file_path = "t-scrapy.db"

    shop_info_table = 'shop_info'
    active_product_table = "shop_product_list"
    product_basic_info_table = "detailed_shop_product_list"
    review_list_table = "shop_review_list"
    date_now = datetime.now().date()
    date_now = pd.to_datetime(date_now)
    if not os.path.exists(file_path):
        print('database not found')
        print('create new database')
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute('''CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                            shopId TEXT,
                                                            reviewId TEXT,
                                                            productName TEXT,
                                                            productPageUrl TEXT,
                                                            productStatus TEXT,
                                                            isDeletedProduct INTEGER,
                                                            productVariantId TEXT,
                                                            productVariantName TEXT,
                                                            rating INTEGER,
                                                            reviewTime TEXT,
                                                            reviewText TEXT,
                                                            reviewerId TEXT,
                                                            reviewerName TEXT,
                                                            avatar TEXT,
                                                            replyText TEXT,
                                                            replyTime TEXT,
                                                            attachmentId TEXT, 
                                                            thumbnailUrl TEXT,
                                                            fullsizeUrl TEXT,
                                                            isReportable INTEGER,
                                                            isAnonymous INTEGER,
                                                            totalLike INTEGER,
                                                            likeStatus INTEGER,
                                                            badRatingReasonFmt TEXT)'''.format(review_list_table))
                                                            
        cursor.execute('''CREATE TABLE IF NOT EXISTS {}
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            description TEXT,
                            domain TEXT,
                            shopId TEXT,
                            name TEXT,
                            nameSearch TEXT,
                            tagLine TEXT,
                            defaultSort INTEGER,
                            dateRecorded TEXT,
                            dateOpen TEXT,
                            totalFavorite INTEGER,
                            alreadyFavorited INTEGER,
                            activeProduct INTEGER,
                            avatar TEXT,
                            cover TEXT,
                            location TEXT,
                            isAllowManage INTEGER,
                            branchLinkDomain TEXT,
                            isOpen INTEGER,
                            shipmentOption TEXT,
                            shippingDistrict TEXT,
                            shippingCity TEXT,
                            productSold INTEGER,
                            totalTxSuccess INTEGER,
                            totalShowcase INTEGER,
                            closedNote TEXT,
                            closedUntil TEXT,
                            closedReason TEXT,
                            isGoldShop INTEGER,
                            isGoldBadgeShop INTEGER,
                            isOfficialShop INTEGER,
                            shopSnippetURL TEXT,
                            titleSEO TEXT,
                            descriptionSEO TEXT,
                            bottomContentSEO TEXT,
                            isQA INTEGER,
                            isGoApotik INTEGER)'''.format(shop_info_table))
                            

        cursor.execute('''CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                        dateRecorded TEXT,
                                                        shopId TEXT,
                                                        productName TEXT,
                                                        productUrl TEXT,
                                                        productId TEXT,
                                                        priceText REAL,
                                                        discounted_percentage INTEGER,
                                                        original_price REAL,
                                                        start_date TEXT,
                                                        end_date TEXT,
                                                        imageOriginal TEXT,
                                                        imageThumbnail TEXT,
                                                        imageResize300 TEXT,
                                                        isSold INTEGER,
                                                        isPreorder INTEGER,
                                                        isWholesale INTEGER,
                                                        storeBadge TEXT,
                                                        reviewCount INTEGER,
                                                        rating INTEGER,
                                                        averageRating REAL,
                                                        category TEXT,
                                                        shopDomain TEXT,
                                                        productKey TEXT,
                                                        extParam TEXT)'''.format(active_product_table))
                                                        

        cursor.execute('''CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                        alias TEXT,
                                                        createdAt TEXT,
                                                        dateRecorded TEXT,
                                                        isQA INTEGER,
                                                        productId TEXT,
                                                        shopId TEXT,
                                                        shopName TEXT,
                                                        minOrder INTEGER,
                                                        maxOrder INTEGER,
                                                        weight INTEGER,
                                                        weightUnit TEXT,
                                                        condition TEXT,
                                                        status TEXT,
                                                        productUrl TEXT,
                                                        isNeedPrescription INTEGER,
                                                        catalogId TEXT,
                                                        isLeasing INTEGER,
                                                        isBlacklisted INTEGER,
                                                        isTokoNow INTEGER,
                                                        etalaseId TEXT,
                                                        etalaseName TEXT,
                                                        etalaseUrl TEXT,
                                                        categoryId TEXT,
                                                        categoryName TEXT,
                                                        categoryTitle TEXT,
                                                        categoryUrl TEXT,
                                                        isAdult TEXT,
                                                        isKyc TEXT,
                                                        minAge TEXT,
                                                        relatedCategoryId TEXT,
                                                        relatedCategoryName TEXT,
                                                        relatedCategoryUrl TEXT,
                                                        transactionSuccess INTEGER,
                                                        transactionReject INTEGER,
                                                        countSold INTEGER,
                                                        paymentVerified INTEGER,
                                                        countView INTEGER,
                                                        countReview INTEGER,
                                                        countTalk INTEGER,
                                                        rating INTEGER,
                                                        productVariantsIdentifier TEXT,
                                                        productVariants TEXT)'''.format(product_basic_info_table))
                                                        
        conn.commit()
        conn.close()

    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    sql_engine = create_engine(f'sqlite:///{file_path}')
    df_shop_info = get_tokopedia_shop_info(shop_url_input)
    shop_id = df_shop_info['shopId'].iloc[0]
    query = f"SELECT * FROM {shop_info_table} WHERE shopId == {df_shop_info['shopId'].iloc[0]}"
    df_existing = pd.read_sql_query(query, sql_engine)
    df_existing['dateRecorded'] = pd.to_datetime(df_existing['dateRecorded'])
    if len(df_existing)<1:
        cursor.execute("BEGIN TRANSACTION")
        queries =  queries = """
        INSERT INTO {} (description,domain,shopId,name,nameSearch,tagLine,
                        defaultSort,dateRecorded,dateOpen,totalFavorite,
                        alreadyFavorited,activeProduct,avatar,cover,location,
                        isAllowManage,branchLinkDomain,isOpen,
                        shipmentOption,shippingDistrict,shippingCity,
                        productSold,totalTxSuccess,totalShowcase,closedNote,
                        closedUntil,closedReason,isGoldShop,isGoldBadgeShop,
                        isOfficialShop,shopSnippetURL,titleSEO,descriptionSEO,
                        bottomContentSEO,isQA,isGoApotik)
                        VALUES (?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?,?,?,?,?,
                                ?,?,?,?,?,?) """.format(shop_info_table)

        params = (df_shop_info["description"].iloc[0],
                  df_shop_info["domain"].iloc[0],
                  df_shop_info["shopId"].iloc[0],
                  df_shop_info["name"].iloc[0],
                  df_shop_info["nameSearch"].iloc[0],
                  df_shop_info["tagLine"].iloc[0],
                  int(df_shop_info["defaultSort"].iloc[0]),
                  str(df_shop_info["dateRecorded"].iloc[0]),
                  str(df_shop_info["dateOpen"].iloc[0]),
                  int(df_shop_info["totalFavorite"].iloc[0]),
                  int(df_shop_info["alreadyFavorited"].iloc[0]),
                  int(df_shop_info["activeProduct"].iloc[0]),
                  df_shop_info["avatar"].iloc[0],
                  df_shop_info["cover"].iloc[0],
                  df_shop_info["location"].iloc[0],
                  int(df_shop_info["isAllowManage"].iloc[0]),
                  df_shop_info["branchLinkDomain"].iloc[0],
                  int(df_shop_info["isOpen"].iloc[0]),
                  df_shop_info["shipmentOption"].iloc[0],
                  df_shop_info["shippingDistrict"].iloc[0],
                  df_shop_info["shippingCity"].iloc[0],
                  int(df_shop_info["productSold"].iloc[0]),
                  int(df_shop_info["totalTxSuccess"].iloc[0]),
                  int(df_shop_info["totalShowcase"].iloc[0]),
                  df_shop_info["closedNote"].iloc[0],
                  str(df_shop_info["closedUntil"].iloc[0]),
                  df_shop_info["closedReason"].iloc[0],
                  int(df_shop_info["isGoldShop"].iloc[0]),
                  int(df_shop_info["isGoldBadgeShop"].iloc[0]),
                  int(df_shop_info["isOfficialShop"].iloc[0]),
                  df_shop_info["shopSnippetURL"].iloc[0],
                  df_shop_info["titleSEO"].iloc[0],
                  df_shop_info["descriptionSEO"].iloc[0],
                  df_shop_info["bottomContentSEO"].iloc[0],
                  int(df_shop_info["isQA"].iloc[0]),
                  int(df_shop_info["isGoApotik"].iloc[0]))
                

        cursor.execute(queries, params)
        conn.commit()
        del query, params, df_existing

    elif len(df_existing)>0 and (df_existing['dateRecorded'].iloc[0] < date_now):
        cursor.execute("BEGIN TRANSACTION")
        queries = """UPDATE {} SET description = ?,domain = ?,shopId = ?,
                                name = ?,nameSearch = ?,tagLine = ?,
                                defaultSort = ?,dateRecorded = ?,dateOpen = ?,
                                totalFavorite = ?,alreadyFavorited = ?,
                                activeProduct = ?,avatar = ?,cover = ?,
                                location = ?,isAllowManage = ?,
                                branchLinkDomain = ?,isOpen = ?,
                                shipmentOption = ?,shippingDistrict = ?,
                                shippingCity = ?,productSold = ?,
                                totalTxSuccess = ?,totalShowcase = ?,
                                closedNote = ?,closedUntil = ?,closedReason = ?,
                                isGoldShop = ?,isGoldBadgeShop = ?,
                                isOfficialShop = ?,shopSnippetURL = ?,titleSEO = ?,
                                descriptionSEO = ?,bottomContentSEO = ?,isQA = ?,
                                isGoApotik = ? WHERE shopId == ?""".format(shop_info_table)

        params = (df_shop_info["description"].iloc[0],
                  df_shop_info["domain"].iloc[0],
                  df_shop_info["shopId"].iloc[0],
                  df_shop_info["name"].iloc[0],
                  df_shop_info["nameSearch"].iloc[0],
                  df_shop_info["tagLine"].iloc[0],
                  int(df_shop_info["defaultSort"].iloc[0]),
                  str(df_shop_info["dateRecorded"].iloc[0]),
                  str(df_shop_info["dateOpen"].iloc[0]),
                  int(df_shop_info["totalFavorite"].iloc[0]),
                  int(df_shop_info["alreadyFavorited"].iloc[0]),
                  int(df_shop_info["activeProduct"].iloc[0]),
                  df_shop_info["avatar"].iloc[0],
                  df_shop_info["cover"].iloc[0],
                  df_shop_info["location"].iloc[0],
                  int(df_shop_info["isAllowManage"].iloc[0]),
                  df_shop_info["branchLinkDomain"].iloc[0],
                  int(df_shop_info["isOpen"].iloc[0]),
                  df_shop_info["shipmentOption"].iloc[0],
                  df_shop_info["shippingDistrict"].iloc[0],
                  df_shop_info["shippingCity"].iloc[0],
                  int(df_shop_info["productSold"].iloc[0]),
                  int(df_shop_info["totalTxSuccess"].iloc[0]),
                  int(df_shop_info["totalShowcase"].iloc[0]),
                  df_shop_info["closedNote"].iloc[0],
                  str(df_shop_info["closedUntil"].iloc[0]),
                  df_shop_info["closedReason"].iloc[0],
                  int(df_shop_info["isGoldShop"].iloc[0]),
                  int(df_shop_info["isGoldBadgeShop"].iloc[0]),
                  int(df_shop_info["isOfficialShop"].iloc[0]),
                  df_shop_info["shopSnippetURL"].iloc[0],
                  df_shop_info["titleSEO"].iloc[0],
                  df_shop_info["descriptionSEO"].iloc[0],
                  df_shop_info["bottomContentSEO"].iloc[0],
                  int(df_shop_info["isQA"].iloc[0]),
                  int(df_shop_info["isGoApotik"].iloc[0]), 
                  shop_id)

        cursor.execute(queries, params)
        conn.commit()
        del query, params, df_existing
    df = get_tokopedia_shop_product(shop_id)
    while len(df)>=1:
        query = f"SELECT * FROM {active_product_table} WHERE shopId == {df['shopId'].iloc[-1]} AND productId == {df['productId'].iloc[-1]}"
        df_existing = pd.read_sql_query(query, sql_engine)
        df_existing['dateRecorded'] = pd.to_datetime(df_existing['dateRecorded'])
        if len(df_existing)==0:
            print(f"add productlist data of {df['productName'].iloc[-1]}")
            cursor.execute("BEGIN TRANSACTION")
            queries = """INSERT INTO {} (dateRecorded,
                                        shopId ,
                                        productName,
                                        productUrl,
                                        productId,
                                        priceText,
                                        discounted_percentage,
                                        original_price,
                                        start_date,
                                        end_date,
                                        imageOriginal,
                                        imageThumbnail,
                                        imageResize300,
                                        isSold,
                                        isPreorder,
                                        isWholesale,
                                        storeBadge,
                                        reviewCount,
                                        rating,
                                        averageRating,
                                        category,
                                        shopDomain,
                                        productKey,
                                        extParam) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".format(active_product_table)

            params = (str(df["dateRecorded"].iloc[-1]),
                      df["shopId"].iloc[-1],
                      df["productName"].iloc[-1],
                      df["productUrl"].iloc[-1],
                      df["productId"].iloc[-1],
                      round(float(df["priceText"].iloc[-1]),2),
                      int(df["discounted_percentage"].iloc[-1]),
                      round(float(df["original_price"].iloc[-1]),2),
                      str(df["start_date"].iloc[-1]),
                      str(df["end_date"].iloc[-1]),
                      df["imageOriginal"].iloc[-1],
                      df["imageThumbnail"].iloc[-1],
                      df["imageResize300"].iloc[-1],
                      int(df["isSold"].iloc[-1]),
                      int(df["isPreorder"].iloc[-1]),
                      int(df["isWholesale"].iloc[-1]),
                      df["storeBadge"].iloc[-1],
                      int(df["reviewCount"].iloc[-1]),
                      int(df["rating"].iloc[-1]),
                      round(float(df["averageRating"].iloc[-1]),2),
                      df["category"].iloc[-1],
                      df["shopDomain"].iloc[-1],
                      df["productKey"].iloc[-1],
                      df["extParam"].iloc[-1])
                    
            
            cursor.execute(queries, params)
            conn.commit()
            del query, params

        elif len(df_existing)==1 and (df_existing['dateRecorded'].iloc[0] < date_now):
            print(f"update productlist data of {df['productName'].iloc[-1]}")
            cursor.execute("BEGIN TRANSACTION")
            queries = """UPDATE {} SET dateRecorded = ?,shopId = ?,
                            productName = ?,productUrl = ?,productId = ?,
                            priceText = ?,discounted_percentage = ?,
                            original_price = ?,start_date = ?,end_date = ?,
                            imageOriginal = ?,imageThumbnail = ?,
                            imageResize300 = ?,isSold = ?,isPreorder = ?,
                            isWholesale = ?,storeBadge = ?,reviewCount = ?,
                            rating = ?,averageRating = ?,category = ?,
                            shopDomain = ?,productKey = ?,extParam = ?
                            WHERE shopId = ?
                            AND productId = ?""".format(active_product_table)

            params = (str(df["dateRecorded"].iloc[-1]),
                      df["shopId"].iloc[-1],
                      df["productName"].iloc[-1],
                      df["productUrl"].iloc[-1],
                      df["productId"].iloc[-1],
                      round(float(df["priceText"].iloc[-1]),2),
                      int(df["discounted_percentage"].iloc[-1]),
                      round(float(df["original_price"].iloc[-1]),2),
                      str(df["start_date"].iloc[-1]),
                      str(df["end_date"].iloc[-1]),
                      df["imageOriginal"].iloc[-1],
                      df["imageThumbnail"].iloc[-1],
                      df["imageResize300"].iloc[-1],
                      int(df["isSold"].iloc[-1]),
                      int(df["isPreorder"].iloc[-1]),
                      int(df["isWholesale"].iloc[-1]),
                      df["storeBadge"].iloc[-1],
                      int(df["reviewCount"].iloc[-1]),
                      int(df["rating"].iloc[-1]),
                      round(float(df["averageRating"].iloc[-1]),2),
                      df["category"].iloc[-1],
                      df["shopDomain"].iloc[-1],
                      df["productKey"].iloc[-1],
                      df["extParam"].iloc[-1],
                    shop_id,df['productId'].iloc[-1])
            cursor.execute(queries, params)
            conn.commit()
            del query, params
        else:
            print(f"productlist data of {df['productName'].iloc[-1]} is up to date")


        query = f"SELECT * FROM {product_basic_info_table} WHERE shopId == {shop_id} AND productId == {df['productId'].iloc[-1]}"
        df_existing = pd.read_sql_query(query, sql_engine)
        df_existing['dateRecorded'] = pd.to_datetime(df_existing['dateRecorded'])
        if len(df_existing)==0:
            print(f"add basic info data of {df['productName'].iloc[-1]}")
            dict_basic_info_data = get_tokopedia_shop_product_basic_info(df.iloc[-1])
            cursor.execute("BEGIN TRANSACTION")
            queries = """INSERT INTO {}
                            (alias,createdAt,dateRecorded,isQA,productId,
                            shopId,shopName,minOrder,maxOrder,weight,
                            weightUnit,condition,status,productUrl,
                            isNeedPrescription,catalogId,isLeasing,
                            isBlacklisted,isTokoNow,etalaseId,etalaseName,
                            etalaseUrl,categoryId,categoryName,
                            categoryTitle,categoryUrl,isAdult,isKyc,
                            minAge,relatedCategoryId,relatedCategoryName,
                            relatedCategoryUrl,transactionSuccess,
                            transactionReject,countSold,paymentVerified,
                            countView,countReview,countTalk,rating,
                            productVariantsIdentifier,productVariants
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                                        ?,?,?,?,?)
                            """.format(product_basic_info_table)

            params = (dict_basic_info_data["alias"].iloc[0],
                        str(dict_basic_info_data["createdAt"].iloc[0]),
                        str(dict_basic_info_data["dateRecorded"].iloc[0]),
                        int(dict_basic_info_data["isQA"].iloc[0]),
                        dict_basic_info_data["productId"].iloc[0],
                        dict_basic_info_data["shopId"].iloc[0],
                        dict_basic_info_data["shopName"].iloc[0],
                        int(dict_basic_info_data["minOrder"].iloc[0]),
                        int(dict_basic_info_data["maxOrder"].iloc[0]),
                        int(dict_basic_info_data["weight"].iloc[0]),
                        dict_basic_info_data["weightUnit"].iloc[0],
                        dict_basic_info_data["condition"].iloc[0],
                        dict_basic_info_data["status"].iloc[0],
                        dict_basic_info_data["productUrl"].iloc[0],
                        int(dict_basic_info_data["isNeedPrescription"].iloc[0]),
                        dict_basic_info_data["catalogId"].iloc[0],
                        int(dict_basic_info_data["isLeasing"].iloc[0]),
                        int(dict_basic_info_data["isBlacklisted"].iloc[0]),
                        int(dict_basic_info_data["isTokoNow"].iloc[0]),
                        dict_basic_info_data["etalaseId"].iloc[0],
                        dict_basic_info_data["etalaseName"].iloc[0],
                        dict_basic_info_data["etalaseUrl"].iloc[0],
                        dict_basic_info_data["categoryId"].iloc[0],
                        dict_basic_info_data["categoryName"].iloc[0],
                        dict_basic_info_data["categoryTitle"].iloc[0],
                        dict_basic_info_data["categoryUrl"].iloc[0],
                        dict_basic_info_data["isAdult"].iloc[0],
                        dict_basic_info_data["isKyc"].iloc[0],
                        dict_basic_info_data["minAge"].iloc[0],
                        dict_basic_info_data["relatedCategoryId"].iloc[0],
                        dict_basic_info_data["relatedCategoryName"].iloc[0],
                        dict_basic_info_data["relatedCategoryUrl"].iloc[0],
                        int(dict_basic_info_data["transactionSuccess"].iloc[0]),
                        int(dict_basic_info_data["transactionReject"].iloc[0]),
                        int(dict_basic_info_data["countSold"].iloc[0]),
                        int(dict_basic_info_data["paymentVerified"].iloc[0]),
                        int(dict_basic_info_data["countView"].iloc[0]),
                        int(dict_basic_info_data["countReview"].iloc[0]),
                        int(dict_basic_info_data["countTalk"].iloc[0]),
                        int(dict_basic_info_data["rating"].iloc[0]),
                        dict_basic_info_data["productVariantsIdentifier"].iloc[0],
                        dict_basic_info_data["productVariants"].iloc[0])
                        
            cursor.execute(queries, params)
            conn.commit()
            del query, params

        elif df_existing['dateRecorded'].iloc[0] < date_now and len(df_existing)==1:
            print(f"update basic info data of {df['productName'].iloc[-1]}")
            dict_basic_info_data = get_tokopedia_shop_product_basic_info(df.iloc[-1])
            cursor.execute("BEGIN TRANSACTION")
            queries = """
            UPDATE {} SET alias = ?,createdAt = ?,dateRecorded = ?,
                isQA = ?,productId = ?,shopId = ?,shopName = ?,
                minOrder = ?,maxOrder = ?,weight = ?,
                weightUnit = ?,condition = ?,status = ?,
                productUrl = ?,isNeedPrescription = ?,catalogId = ?,
                isLeasing = ?,isBlacklisted = ?,isTokoNow = ?,
                etalaseId = ?,etalaseName = ?,etalaseUrl = ?,
                categoryId = ?,categoryName = ?,categoryTitle = ?,
                categoryUrl = ?,isAdult = ?,isKyc = ?,minAge = ?,
                relatedCategoryId = ?,relatedCategoryName = ?,
                relatedCategoryUrl = ?,transactionSuccess = ?,
                transactionReject = ?,countSold = ?,
                paymentVerified = ?,countView = ?,countReview = ?,
                countTalk = ?,rating = ?,
                productVariantsIdentifier = ?,productVariants = ?
                    WHERE shopId = ? AND productId = ?
            """.format(product_basic_info_table)

            params = (dict_basic_info_data["alias"].iloc[0],
                        str(dict_basic_info_data["createdAt"].iloc[0]),
                        str(dict_basic_info_data["dateRecorded"].iloc[0]),
                        int(dict_basic_info_data["isQA"].iloc[0]),
                        dict_basic_info_data["productId"].iloc[0],
                        dict_basic_info_data["shopId"].iloc[0],
                        dict_basic_info_data["shopName"].iloc[0],
                        int(dict_basic_info_data["minOrder"].iloc[0]),
                        int(dict_basic_info_data["maxOrder"].iloc[0]),
                        int(dict_basic_info_data["weight"].iloc[0]),
                        dict_basic_info_data["weightUnit"].iloc[0],
                        dict_basic_info_data["condition"].iloc[0],
                        dict_basic_info_data["status"].iloc[0],
                        dict_basic_info_data["productUrl"].iloc[0],
                        int(dict_basic_info_data["isNeedPrescription"].iloc[0]),
                        dict_basic_info_data["catalogId"].iloc[0],
                        int(dict_basic_info_data["isLeasing"].iloc[0]),
                        int(dict_basic_info_data["isBlacklisted"].iloc[0]),
                        int(dict_basic_info_data["isTokoNow"].iloc[0]),
                        dict_basic_info_data["etalaseId"].iloc[0],
                        dict_basic_info_data["etalaseName"].iloc[0],
                        dict_basic_info_data["etalaseUrl"].iloc[0],
                        dict_basic_info_data["categoryId"].iloc[0],
                        dict_basic_info_data["categoryName"].iloc[0],
                        dict_basic_info_data["categoryTitle"].iloc[0],
                        dict_basic_info_data["categoryUrl"].iloc[0],
                        dict_basic_info_data["isAdult"].iloc[0],
                        dict_basic_info_data["isKyc"].iloc[0],
                        dict_basic_info_data["minAge"].iloc[0],
                        dict_basic_info_data["relatedCategoryId"].iloc[0],
                        dict_basic_info_data["relatedCategoryName"].iloc[0],
                        dict_basic_info_data["relatedCategoryUrl"].iloc[0],
                        int(dict_basic_info_data["transactionSuccess"].iloc[0]),
                        int(dict_basic_info_data["transactionReject"].iloc[0]),
                        int(dict_basic_info_data["countSold"].iloc[0]),
                        int(dict_basic_info_data["paymentVerified"].iloc[0]),
                        int(dict_basic_info_data["countView"].iloc[0]),
                        int(dict_basic_info_data["countReview"].iloc[0]),
                        int(dict_basic_info_data["countTalk"].iloc[0]),
                        int(dict_basic_info_data["rating"].iloc[0]),
                        dict_basic_info_data["productVariantsIdentifier"].iloc[0],
                        dict_basic_info_data["productVariants"].iloc[0],
                        shop_id,
                        dict_basic_info_data['productId'].iloc[0])
            cursor.execute(queries, params)
            conn.commit()
            del query, params,dict_basic_info_data
        else:
            print(f"basic info data of {df['productName'].iloc[-1]} is up to date")

        df.drop([(len(df)-1)],inplace=True)

    df = get_review_data(url=shop_url_input,shopId = shop_id)
    while len(df)>=1:
        query = f"SELECT * FROM {review_list_table} WHERE shopId == {shop_id} AND reviewId == {df['reviewId'].iloc[-1]}"
        df_existing = pd.read_sql_query(query, sql_engine)
        if len(df_existing)==0:
            print(f"add review data with id {df['reviewId'].iloc[-1]}")
            cursor.execute("BEGIN TRANSACTION")
            queries = """INSERT INTO {} (shopId,reviewId,productName,productPageUrl,
                                        productStatus,isDeletedProduct,productVariantId,
                                        productVariantName,rating,reviewTime,reviewText,
                                        reviewerId,reviewerName,avatar,replyText,replyTime,
                                        attachmentId,thumbnailUrl,fullsizeUrl,
                                        isReportable,isAnonymous,totalLike,likeStatus,
                                        badRatingReasonFmt) 
                                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".format(review_list_table)
            
            params = (df['shopId'].iloc[-1],
                        df['reviewId'].iloc[-1],
                        df['productName'].iloc[-1],
                        df['productPageUrl'].iloc[-1],
                        str(df['productStatus'].iloc[-1]),
                        int(df['isDeletedProduct'].iloc[-1]),
                        df['productVariantId'].iloc[-1],
                        df['productVariantName'].iloc[-1],
                        int(df['rating'].iloc[-1]),
                        df['reviewTime'].iloc[-1],
                        df['reviewText'].iloc[-1],
                        df['reviewerId'].iloc[-1],
                        df['reviewerName'].iloc[-1],
                        df['avatar'].iloc[-1],
                        df['replyText'].iloc[-1],
                        df['replyTime'].iloc[-1],
                        df['attachmentId'].iloc[-1],
                        df['thumbnailUrl'].iloc[-1],
                        df['fullsizeUrl'].iloc[-1],
                        int(df['isReportable'].iloc[-1]),
                        int(df['isAnonymous'].iloc[-1]),
                        int(df['totalLike'].iloc[-1]),
                        int(df['likeStatus'].iloc[-1]),
                        df['badRatingReasonFmt'].iloc[-1])
            
            cursor.execute(queries, params)
            conn.commit()
        elif len(df_existing)==1:
            print(f"update review data with id {df['reviewId'].iloc[-1]}")
            cursor.execute("BEGIN TRANSACTION")
            queries = """UPDATE {} SET shopId = ?,
                        reviewId = ?,
                        productName = ?,
                        productPageUrl = ?,
                        productStatus = ?,
                        isDeletedProduct = ?,
                        productVariantId = ?,
                        productVariantName = ?,
                        rating = ?,
                        reviewTime = ?,
                        reviewText = ?,
                        reviewerId = ?,
                        reviewerName = ?,
                        avatar = ?,
                        replyText = ?,
                        replyTime = ?,
                        attachmentId = ?,
                        thumbnailUrl = ?,
                        fullsizeUrl = ?,
                        isReportable = ?,
                        isAnonymous = ?,
                        totalLike = ?,
                        likeStatus = ?,
                        badRatingReasonFmt = ? WHERE shopId == ? AND reviewId == ?""".format(review_list_table)

            params = (df['shopId'].iloc[-1],
                        df['reviewId'].iloc[-1],
                        df['productName'].iloc[-1],
                        df['productPageUrl'].iloc[-1],
                        str(df['productStatus'].iloc[-1]),
                        int(df['isDeletedProduct'].iloc[-1]),
                        df['productVariantId'].iloc[-1],
                        df['productVariantName'].iloc[-1],
                        int(df['rating'].iloc[-1]),
                        df['reviewTime'].iloc[-1],
                        df['reviewText'].iloc[-1],
                        df['reviewerId'].iloc[-1],
                        df['reviewerName'].iloc[-1],
                        df['avatar'].iloc[-1],
                        df['replyText'].iloc[-1],
                        df['replyTime'].iloc[-1],
                        df['attachmentId'].iloc[-1],
                        df['thumbnailUrl'].iloc[-1],
                        df['fullsizeUrl'].iloc[-1],
                        int(df['isReportable'].iloc[-1]),
                        int(df['isAnonymous'].iloc[-1]),
                        int(df['totalLike'].iloc[-1]),
                        int(df['likeStatus'].iloc[-1]),
                        df['badRatingReasonFmt'].iloc[-1],
                        shop_id,df['reviewId'].iloc[-1])
        df.drop([(len(df)-1)],inplace=True)
    


    #reviewData.to_sql(review_list_table, con=sql_engine, index=False, if_exists='replace')
    conn.close()
    #print('update list review table')

if __name__ == "__main__":
    main()
