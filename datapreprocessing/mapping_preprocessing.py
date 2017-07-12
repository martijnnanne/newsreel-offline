

class Mapping():
    def __init__(self):
        self.simple_keys = [4, 5, 6, 7, 9, 12, 13, 14,
                            15, 16, 17, 18, 19, 20, 21,
                            22, 23, 24, 25, 26, 27, 28,
                            29, 30, 31, 34, 35, 36, 37,
                            38, 39, 40, 41, 42, 43, 44,
                            45, 47, 48, 49, 50, 52, 53,
                            54, 55, 56, 57, 59]
        self.cluster_keys = [1, 2, 3, 33, 51]
        self.list_keys = [8, 10, 11, 46, 58]

        self.simple_header = ['BROWSER','ISP','OS','GEO_USER','TIME_WEEKDAY','POSITION',
                              'DO_NOT_TRACK','WIDGET_KIND','WEATHER','GEO_PUBLISHER','LANG_USER','POSITION_IN_WIDGET',
                              'SUBID','TIME_TO_ACTION','WIDGET_PAGE','GEO_USER_ZIP','TIME_HOUR','USER_PUBLISHER_IMPRESSION',
                              'ITEM_SOURCE', 'RETARGETING','PUBLISHER','USER_CAMPAIGN_IMPRESSION', 'SSP',
                              'PIXEL_3RD_PARTY','GEO_USER_RADIUS','REDUCED_BID','ADBLOCKER','BID_CPM',
                              'RELEASE','PRESENTDAY','WIDGET_ID','DIMENSION_SCREEN','CONTEST_TEAM','ITEM_STATUS',
                              'PIXEL_4TH_PARTY','PUBLISHER_REFEER', 'SSP_PUBLISHERID','DEVICE_TYPE','GEO_TYPE',
                              'TIME_MINUTE_30','CPO', 'FILTER_ALLOWOSR','URL','SSP_QUALIFIER','SSP_NETWORK',
                              'BROWSER_3RD_PARTY_SUPPORT','USER_COOKIE','TRANSPORT_PROTOCOL']

        self.cluster_header = ['GENDER', 'AGE', 'INCOME', 'KEYWORD', 'ITEM_AGE']
        self.list_header = ['PUBLISHER_FILTER', 'CHANNEL','CATEGORY', 'CATEGORY_SEM','CHANNEL_SEM']


        self.item_update_keys = ['id', 'domainid', 'created_at', 'updated_at', 'flag', 'title', 'text', 'url',
                                 'img', 'version']

        self.timestamp = ["timestamp"]
        self.recs = ["recs"]
        self.type = ["type"]
        self.limit = ["limit"]

        self.publishers = [35774, 1677, 13554, 418, 694, 3336, 2522]

        self.publisher_mapping = {418:'ksta', 1677:'tagesspiegel', 35774:'sport1', 694:'gulli', }

    def get_header_rec(self):
        return self.simple_header + self.cluster_header + self.list_header + self.limit

    def get_header_event(self):
        return self.simple_header + self.cluster_header + self.list_header + self.recs + self.timestamp + self.type + self.limit

    def get_header_update(self):
        return self.item_update_keys
