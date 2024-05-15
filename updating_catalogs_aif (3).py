import sys
from mediascope.bdp_python_utils.de.transformations.spark import TransformationSpark
from mediascope.bdp_python_utils.de.common import logger
from mediascope.bdp_python_utils.common._data_operations import str_to_bool
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StringType
import pandas as pd
import time
import yake
import hashlib
import itertools
from string import punctuation
import re
from transliterate import slugify
from datetime import datetime, timedelta
import os 
import datetime
import pymongo
from clickhouse_driver import Client

REQUIRED_PARAMS = [
    'TransformationName',
    'Output.ClickHouse.schema1',
    'Output.ClickHouse.schema2',
    'Output.ClickHouse.schema3',
    'Output.ClickHouse.table1',
    'Output.ClickHouse.table2',
    'Output.ClickHouse.table3',
    'Output.ClickHouse.table4',
    'Output.ClickHouse.table5', 
    'Output.ClickHouse.table6', 
    'Output.ClickHouse.table7', 
    'Output.ClickHouse.table8',
    'Output.ClickHouse.table9'
]


DEV_PARAMS = [
]

OPTIONAL_PARAMS = [
]


parn = [ 'ivi', 'ntv', '_ctc_tv', 'kinopoisk', 'rostelecom', 'tricolor', 'beeline', 'okko', '1tv', 'kino.1tv', 'smotrim', 'mts', 'premier_one', 'tnt_tv',  'start', 'amediateka']
part = {
    'ntv' : {'cat' : 126}, 
    '_ctc_tv' : {'cat' : 173},
    'kinopoisk' : {'cat' : 119}, 
    'rostelecom' : {'cat' : 182}, 
    'okko' : {'cat' : 118},
    'ivi': {'cat' : 109}, 
    '1tv': {'cat' : 201}, 
    'kino.1tv': {'cat' : 124}, 
    'smotrim': {'cat' : 113}, 
    'mts': {'cat' : 111}, 
    'premier_one': {'cat' : 103}, 
    'tnt_tv' : {'cat' : 175},  
    'start' : {'cat' : 116},  
    'amediateka' : {'cat' : 114},
    'beeline': {'cat' : 105}, 
    'tricolor': {'cat' : 110}, 
    '_more_tv' : {'cat' : 117}
    }

class query_CH_test(TransformationSpark):
    def __init__(
            self,
            **kwargs,
    ):
        super().__init__(**kwargs)


    def execute(self):
        
        # clickhouse_driver-session
        def clickhouse_driver_client():
            client = Client(host=self.args.get('clickhouse.host'),
                            port=self.args.get('clickhouse.http_port'),
                            user=self.args.get('clickhouse.user'),
                            password=self.args.get('clickhouse.password'))
                            #database=self.args.get('Output.ClickHouse.schema'))
            return client

        logger.info(f'Подключение к КликХаусу')
        client = clickhouse_driver_client()

        logger.info(f'Подгружаю все таблицы')
        # Включаем использование Clickhouse
        new_catalog = self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')
        local_art = self.args.get('Output.ClickHouse.schema2') + '.' + self.args.get('Output.ClickHouse.table2')
        contacts = self.args.get('Output.ClickHouse.schema3') + '.' + self.args.get('Output.ClickHouse.table3')
        kp_base = self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table4')
        update_log = self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table5')
        country = self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table6')
        trailers = self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table7')
        remarka = self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table8')
        new_catalog_up = self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table9')

        logger.info(f'Подключаюсь к Mongo')
        conn = pymongo.MongoClient('mongodb://metrixuser01:Jv9rkjjwFFzfhyTalviv@devdvinmongodb02.aeroport.tns:27017/metrix?authSource=metrix')
        db = conn['metrix']

        def inCat(zz): 
           client.execute(f'INSERT INTO {self.args.get("Output.ClickHouse.schema1") + "." + self.args.get("Output.ClickHouse.table9")} ("hashKey", "hashPRJKey", "OriginalItemID", "catalogid", "FullTitle", "MasterTitle", "IsSeries", "Project", "SeasonNum", "EpisodeNum", "ProdYear", "ProdCountry", "isoCountry", "uploadtime", "editor") VALUES', zz)
        def inCatz(zz):  
           client.execute(f'INSERT INTO {self.args.get("Output.ClickHouse.schema1") + "." + self.args.get("Output.ClickHouse.table9")} ("IsMaster", "hashKey", "hashPRJKey", "OriginalItemID", "catalogid", "FullTitle", "MasterTitle", "IsSeries", "Project", "SeasonNum", "EpisodeNum", "ProdYear", "ProdCountry", "isoCountry", "uploadtime", "editor", "IsTrailer", "Duration", "ReleaseDate") VALUES', zz)
        def inLog(zz): 
           client.execute(f'INSERT INTO {self.args.get("Output.ClickHouse.schema1") + "." + self.args.get("Output.ClickHouse.table5")} ("editor", "partner_name", "catalogid", "count_OriginalItemID", "uploadtime") VALUES', zz)
        
        logger.info('Готовим страны')
        countrys = {}
        qс =f"SELECT iso, name FROM {country}"
        lst = client.execute(qс)
        dfpс = pd.DataFrame(lst, columns=["iso", "name"])
        for k, v in dfpс.iterrows():
            iso = v['iso']
            country = v['name']
            countrys.update({country : iso})

        def prodiso(z):
            try:
                return countrys[z]
            except:
                return ''
            
        qw = f"SELECT DISTINCT ProdCountry AS Country FROM {local_art} WHERE ProdCountry != ' ' "
        lst = client.execute(qw)
        dfpw = pd.DataFrame(lst, columns=["Country"])
        rate = {}
        ratec = []
        isob = []

        def corate(ca):
            try:
                if ca in ratec:
                    r = rate[ca]
                    rate.update({ca: r+1})
                else: 
                    rate.update({ca: 1})
                    ratec.append(ca) 
            except:
                return ''
                
        def makerate():
            for k, v in dfpw.iterrows():
                x = v['Country'].lower() 
                x = x.replace('"','')
                if '[' in x: x =  x[1:-1]
                if ',' in x:
                    for c in x.split(','):
                        ca = c.strip()
                        tca = ca
                        if len(ca)>2: 
                            ca = prodiso(ca)
                            if len(ca)>2: 
                                
                                if ca not in isob: isob.append(ca)
                        corate(ca)             
                else:
                    ca = x.strip()
                    tca = ca 
                    if len(ca)>2: 
                        ca = prodiso(ca)
                        if len(ca)>2: 
                            
                            if ca not in isob: isob.append(ca)
                    corate(ca)

        makerate()

        def base(li):
            sd = dict(sorted(li.items(), key=lambda x:x[1], reverse=True))
            return [elem[0] for elem in sd.items()][0]

        def urate(xx):
            ser = ''
            ca = xx.strip() 
            ca = prodiso(ca) 
            
            if len(ca)>2: 
                try: 
                    rxx.update({ca: rate[ca]})
                    ser = base(rxx)
                    
                except:
                    ser = ''
            elif len(ca)==2: ser = ca
            return ser

        def getrate(x):
            try:
                r = rate[x]
            except: r = 0
            return r

        def countryList(xx):
            if '[' in xx:  
                xx =  xx[1:-1] 
                xx = xx.replace('"','').replace("'",'').replace(",",', ').lower() 
            return xx

        def getiso(xx): 
            ser = ''
            maxrate = 0
            if xx != 'none':
                rxx = {}
                xx = xx.replace('"','').replace("'",'').lower() 
                if '[' in xx:  xx =  xx[1:-1]   
                
                if ',' in xx:
                    
                    for c in xx.split(','): 
                        ca = c.strip()
                        
                        if len(ca)==2: 
                            carate = getrate(ca)
                            
                            if carate > maxrate:
                                ser = ca
                                maxrate = carate
                        else: 
                            zz = urate(ca)
                            carate = getrate(zz)
                            
                            if carate > maxrate:
                                ser = zz
                                maxrate = carate
                                
                else:
                    if len(xx)==2: ser = xx
                    else:
                        ser = urate(xx)
                        
            return ser   

        logger.info('Готовим Hash')     
        myake = yake.KeywordExtractor(lan="ru", n=2, dedupLim=0.7, dedupFunc = 'seqm', windowsSize=1, top=5, features=None)
        sim_stop = ['сезон', 'анонс', 'мультфильм', 'эфир', 'художественный', 'фильм', 'ultra', 'None', 'nan', '<NA>', 'null', 'серия', '4k', 'hd', 'эпизод', 'часть', 'rtv', 'амедиатека', 'кинопоиск', 'выпуск', 'amediateka']

        def emoji_clean(txt):
            emoji_pattern = re.compile("["
                    u"\U0001F600-\U0001F64F"  # emoticons
                    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                    u"\U0001F680-\U0001F6FF"  # transport & map symbols
                    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                    u"\xa0"
                                    "]+", flags=re.UNICODE)
            return emoji_pattern.sub(r'', txt) # no emojidef emoji_clean(txt):
        
        def clear_txt(txt):
            remove = punctuation #.replace("!", " ").replace("?", " ")
            pattern = r"[{}]".format(remove)
            return re.sub(pattern, " ", txt)

        def preprocess_text(z, regexp):
                z = z.lower().replace('-я', '').replace('\xa0',' ')
                result = ''
                add = []
                z = emoji_clean(z).replace("-", " ") 
                z = clear_txt(z).replace('ё', 'е').replace('э', 'е')
                tokens = z.replace('&',' ').replace('"',' ').replace('«','').replace('»',' ').replace('\xa0',' ').split(' ')
                
                tokens = [token for token in tokens if  token != " " and token.strip() not in punctuation]
                t = numz(tokens, regexp)
                
                if len(t)>0: 
                    t.sort()
                    result = str(' '.join(t))
                return result
            
        def numz(array, regexp):
                result=[]
                for w in array:
                    if w is not False and re.search(regexp, w) is not None and len(w)>0 and w not in sim_stop and w not in result:  result.append(w)     
                return result
        
            
        def run_yake(text_content, regexp):
            table = []
            ax = []
            pt = preprocess_text(text_content, regexp)
            ax.append(pt)
            for kw in myake.extract_keywords(pt): 
                table.append({"keyword":kw[0], "score":kw[1]})
                if kw[1]> 0.04 and kw[0] not in ax:  ax.append(kw[0])
                    
            txt = str('-'.join(ax)).replace('-','')
            
            if len(txt)<3: return 'nohash', txt
            else: 
                if re.search(r'[а-яА-Я]', txt) is not None: txt = slugify(txt) 
                try: 
                    md5 = hashlib.md5(txt.encode('utf-8')).hexdigest()
                    return md5, txt
                except:
                    return 'nohash', txt

        def getHash(Title, SeasonNum, EpisodeNum, Project, iso, ProdYear, IsSeries):
            iss = ''
            sn = ''
            en = ''
            hashPRJ_txt = 'nohash'
            hash_txt = 'nohash'
            
            ft= Title.replace("'", "").replace('"', "").lower() 

            if len(str(SeasonNum))>0 and SeasonNum is not None: 
                if re.search(r'[0-9]', str(SeasonNum)) is not None and int(SeasonNum)>0: sn = 'alpha'+ str(SeasonNum).replace('.0','')
            else: sn = Title.replace('сезон ','alpha')

            if len(str(EpisodeNum))>0 and EpisodeNum is not None:
                if re.search(r'[0-9]', str(EpisodeNum)) is not None and int(EpisodeNum)>0: en = 'omega'+str(EpisodeNum).replace('.0','')
            else: en = Title.replace('серия ','omega')

            if IsSeries == 'true' or IsSeries == '1' or IsSeries == True or IsSeries == 1 or IsSeries is True:
                ft = str(ProdYear)+' '+Project+' '+ft+' '+sn+' '+en+' '+iso 
            else:
                ft = str(ProdYear)+' '+ft+' '+sn+' '+en+' '+iso 
            
            if len(ft) > 0: 
                a = run_yake(ft, r'[a-zA-Zа-яА-Я0-9]')
                hash_txt = a[0]
            else:
                hash_txt = 'nohash' 
                
            if IsSeries == 'true' or IsSeries == '1' or IsSeries == True or IsSeries == 1: iss = ' betaseries'

            if Project != 'None' and Project is not None and len(Project)>0:
                
                if re.search(r'[0-9]', Project) is not None and re.search(r'[a-zа-я]', Project) is None:
                    PRJCountry = Project.replace("'", "").replace('"', "").replace(' ', "").lower() +' Country'+iso+iss
                    
                    hashPRJ_txt = hashlib.md5(PRJCountry.encode('utf-8')).hexdigest()
                else:
                    PRJCountry = Project+' Country'+iso+iss
                    b= run_yake(PRJCountry, r'[a-zA-Zа-яА-Я]')
                    if b is not None: 
                        hashPRJ_txt = b[0]+iso 
                        hashPRJ_txt = hashPRJ_txt.replace(" ", "")
                    else: 
                        hashPRJ_txt = 'nohash'
                    
            return hash_txt, hashPRJ_txt

        logger.info(f'Проверка в Mongo')
        def mgetHahs(h):
            co = db['UnOfficial_KP']
            x = False
            result = co.find_one({"hash": h})
            if result: x = True
            else:
                co.update_one({"hash": h}, {'$set': {'day': datetime.datetime.now(tz=None)}}, upsert=True)
            return x

        def mgetContactsHahs(h):
            co = db['Contacts']
            x = False
            result = co.find_one({"hash": h})
            if result: x = True
            else:
                co.update_one({"hash": h}, {'$set': {'day': datetime.datetime.now(tz=None)}}, upsert=True)
            return x

        def mgetCatHahs(h):
            co = db['CatalogHath']
            x = False
            result = co.find_one({"hash": h})
            if result: x = True
            else:
                co.update_one({"hash": h}, {'$set': {'day': datetime.datetime.now(tz=None)}}, upsert=True)
            return x    
         
        logger.info(f'Готовим new данные по контактам из SDM_Metrix_Contact (Шаг 1)')
        daynow = datetime.datetime.now() - datetime.timedelta(days=6)
        dayup = str(daynow.strftime("%Y-%m-%d"))

        def GetContacts2(day, catalogid): 
            q = f"WITH \
            FA AS ( \
                SELECT DISTINCT OriginalItemID, catalogid, MAX(datadate) AS uploadtime \
                FROM {self.args.get('Output.ClickHouse.schema3') + '.' + self.args.get('Output.ClickHouse.table3')} \
                WHERE datadate > '"+day+"' AND \
                catalogid = "+str(catalogid)+f" \
                AND OriginalItemID NOT LIKE 'adv_%' \
                GROUP BY OriginalItemID, catalogid  ) \
            SELECT DISTINCT OriginalItemID, catalogid, FullTitle, Title, IsSeries, \
            if(notEmpty(Project), Project, '') Project, \
            SeasonNum,   EpisodeNum,  ProdYear,  ProdCountry   \
            FROM {self.args.get('Output.ClickHouse.schema3') + '.' + self.args.get('Output.ClickHouse.table3')} AS A \
            JOIN FA ON A.OriginalItemID = FA.OriginalItemID AND A.catalogid = FA.catalogid AND A.datadate = FA.uploadtime \
            WHERE datadate > '"+day+"' AND catalogid = "+str(catalogid)+" \
            AND countMatches(Title, '[а-яА-Я]' ) = 0 and countMatches(Project, '[а-яА-Я]' ) >0 \
            ORDER BY A.datadate DESC"  
            lst = client.execute(q) 
            dfp = pd.DataFrame(lst, columns=["OriginalItemID", "catalogid", "FullTitle", "Title", "IsSeries", "Project", "SeasonNum", "EpisodeNum", "ProdYear", "ProdCountry"])
            return dfp

        def mainContacts2(day,catalogid):
            
            dfp = GetContacts2(day,catalogid)
            total = len(dfp)
            
            n = 0
            sds = [] 
            hashCO = []
            uploadtime = datetime.datetime.now(tz=None)
            m = 10000 
            
            
            editor = 'Contacts '+day
            
            if total>0:
                for index, row in dfp.iterrows():  
                    
                    OriginalItemID = row['OriginalItemID']  
                    catalogid = row['catalogid'] 
                    iso = ''
                    PRJCountry = ''
                    ft = ''
                    SeasonNum = ''
                    EpisodeNum = ''
                    ProdYear = ''
                    iss = ''
                    MasterTitle = row['Project'].replace('"','')
                    IsSeries = row['IsSeries']
                    catalogid = row['catalogid'] 
                    FullTitle = row['FullTitle'] 
                    Project = row['Project'].replace('"','').capitalize()
                    SeasonNum = row['SeasonNum'] 
                    EpisodeNum = row['EpisodeNum'] 
                    ProdYear = str(row['ProdYear']) 
                    Country = str(row['ProdCountry']).lower() 
                    
                    if FullTitle != FullTitle or FullTitle is None or str(FullTitle) in ['None', 'nan', '<NA>', 'null']: FullTitle = ''
                    else:  FullTitle = FullTitle.replace("'", "").replace('"', "") 
                    
                    if SeasonNum == SeasonNum  and SeasonNum is not None and str(SeasonNum) not in ['None', 'nan', '<NA>', 'null']:
                        if len(str(SeasonNum))>0 and str(SeasonNum).replace('.0','') != '0':
                            SeasonNum = str(SeasonNum).replace('.0','')
                            IsSeries = True
                        else: SeasonNum = ''
                    else: SeasonNum = ''
                    
                    if EpisodeNum == EpisodeNum and len(str(EpisodeNum))>0 and EpisodeNum is not None and str(EpisodeNum) not in ['None', 'nan', '<NA>', 'null']:
                        if len(str(EpisodeNum))>0 and str(EpisodeNum).replace('.0','') != '0':
                            EpisodeNum = str(EpisodeNum).replace('.0','')
                            IsSeries = True
                        else: EpisodeNum = ''
                    else: EpisodeNum = ''
                    
                    
                    if ProdYear == ProdYear and ProdYear is not None and str(ProdYear) not in ['None', 'nan', '<NA>', 'null', '']:
                        if len(str(ProdYear))>0 and  '.0' in str(ProdYear): ProdYear = str(ProdYear).replace('.0','')
                    else:  ProdYear = ''
                    
                    if Country == Country and Country is not None and len(Country)>0:  
                        sol = Country.strip()   
                        if len(sol)>2 and Country !='none': iso = getiso(sol)
                        elif len(sol)>2 and Country =='none': 
                            iso = ''
                            Country = ''
                        else: iso = sol
                    if '[' in iso: iso = ''
                    
                    if MasterTitle == MasterTitle and MasterTitle is not None and str(MasterTitle) not in ['None', 'nan', '<NA>', 'null']:  
                        if len(MasterTitle)>0 and type(MasterTitle) != 'NoneType': MasterTitle= MasterTitle.replace("'", "").replace('"', "")
                        ft = MasterTitle
                    else: 
                        MasterTitle = ''
                    
                    hashKey = 'nohash'
                    hashPRJKey = 'nohash'
                    
                    hashs = getHash(MasterTitle, SeasonNum, EpisodeNum, Project, iso, ProdYear, IsSeries)
                    
                    if len(MasterTitle)>0: hashKey = hashs[0]
                    if len(Project)>0: hashPRJKey = hashs[1]
                        
                    if len(MasterTitle)>0 or len(Project)>0:
                        uploadtime = datetime.datetime.now(tz=None)#### - datetime.timedelta(days=300)
                        
                    n+=1
                    
                    sd = {
                        "hashKey" : hashKey, 
                        "hashPRJKey" : hashPRJKey,
                        "OriginalItemID":OriginalItemID,
                        "catalogid": catalogid,
                        "FullTitle": FullTitle,
                        "MasterTitle": MasterTitle,
                        "IsSeries" : IsSeries,
                        "Project": Project,
                        "SeasonNum": SeasonNum,
                        "EpisodeNum": EpisodeNum,
                        "ProdYear": ProdYear,
                        "ProdCountry": countryList(Country.strip()),
                        "isoCountry": iso, 
                        "uploadtime": uploadtime,
                        "editor": editor}
                    
                    CO = str(row['OriginalItemID'])+str(row['catalogid'])
                    hCO = hashlib.md5(CO.encode('utf-8')).hexdigest()
                    
                    if hCO not in hashCO:
                        sds.append(sd)
                        hashCO.append(hCO)  

                    if n%m == 0:  
                        inCat(sds) 
                        sds = []
                    
                if len(sds) > 0: 
                    inCat(sds) 
            
            return int(total)
            
        def ContactA(pps):
            logs =[]
            for p in pps:
                catID = part[p]['cat'] 
                
                for i in range(0,1):
                    z = mainContacts2(dayup, catID) 
                    log = {
                        'editor' :'Contacts',
                        'partner_name' : p,
                        'catalogid' : catID,
                        'count_OriginalItemID': z,
                        'uploadtime': datetime.datetime.now(tz=None)
                    }
                    logs.append(log)
                    if z <10000: break
                        
            if len(logs)>0: inLog(logs)	 

        logger.info(f'Готовим new данные по контактам из SDM_Metrix_Contact (Шаг 2)') 
        def GetContacts(day, catalogid): 
            q = f"WITH \
            FA AS ( \
                SELECT DISTINCT OriginalItemID, catalogid, MAX(datadate) AS uploadtime \
                FROM {self.args.get('Output.ClickHouse.schema3') + '.' + self.args.get('Output.ClickHouse.table3')} \
                WHERE datadate > '"+day+"' AND \
                catalogid = "+str(catalogid)+f" \
                AND OriginalItemID NOT LIKE 'adv_%' \
                GROUP BY OriginalItemID, catalogid  ) \
            SELECT DISTINCT OriginalItemID, catalogid, FullTitle, Title, IsSeries, \
            if(notEmpty(Project), Project, '') Project, \
            SeasonNum,   EpisodeNum,  ProdYear, ProdCountry   \
            FROM {self.args.get('Output.ClickHouse.schema3') + '.' + self.args.get('Output.ClickHouse.table3')} AS A \
            JOIN FA ON A.OriginalItemID = FA.OriginalItemID AND A.catalogid = FA.catalogid AND A.datadate = FA.uploadtime \
            WHERE datadate > '"+day+"' AND catalogid = "+str(catalogid)+"  " 
            lst = client.execute(q)  
            dfp = pd.DataFrame(lst, columns=["OriginalItemID", "catalogid", "FullTitle", "Title", "IsSeries", "Project", "SeasonNum", "EpisodeNum", "ProdYear", "ProdCountry"])
            return dfp
        

        def mainContacts(day,catalogid):
            
            dfp = GetContacts(day,catalogid) 
            total = len(dfp) 
            n = 0
            sds = [] 
            hashCO = [] 
            uploadtime = datetime.datetime.now(tz=None)
            m = 10000 
            
            dayup = datetime.datetime.now(tz=None).strftime('%Y-%m-%d')
            editor = 'Contacts '+dayup
            
            if total>0:
                for index, row in dfp.iterrows(): 
                    
                    cuhash = hashlib.md5(str(row).encode('utf-8')).hexdigest()
                    
                    if mgetContactsHahs(cuhash) == False:
                    
                        OriginalItemID = row['OriginalItemID']  
                        catalogid = row['catalogid'] 
                        iso = ''
                        PRJCountry = ''
                        ft = ''
                        SeasonNum = ''
                        EpisodeNum = ''
                        ProdYear = ''
                        iss = ''
                        MasterTitle = row['Title'] 
                        IsSeries = row['IsSeries']
                        catalogid = row['catalogid'] 
                        FullTitle = row['FullTitle'] 
                        Project = row['Project'].replace('"','').capitalize()
                        SeasonNum = row['SeasonNum'] 
                        EpisodeNum = row['EpisodeNum'] 
                        ProdYear = str(row['ProdYear']) 
                        Country = str(row['ProdCountry']).lower() 

                        if FullTitle != FullTitle or FullTitle is None or str(FullTitle) in ['None', 'nan', '<NA>', 'null']: FullTitle = ''
                        else:  FullTitle = FullTitle.replace("'", "").replace('"', "") 

                        if SeasonNum == SeasonNum  and SeasonNum is not None and str(SeasonNum) not in ['None', 'nan', '<NA>', 'null']:
                            if len(str(SeasonNum))>0 and str(SeasonNum).replace('.0','') != '0':
                                SeasonNum = str(SeasonNum).replace('.0','')
                                IsSeries = True
                            else: SeasonNum = ''
                        else: SeasonNum = ''

                        if EpisodeNum == EpisodeNum and len(str(EpisodeNum))>0 and EpisodeNum is not None and str(EpisodeNum) not in ['None', 'nan', '<NA>', 'null']:
                            if len(str(EpisodeNum))>0 and str(EpisodeNum).replace('.0','') != '0':
                                EpisodeNum = str(EpisodeNum).replace('.0','')
                                IsSeries = True
                            else: EpisodeNum = ''
                        else: EpisodeNum = ''


                        if ProdYear == ProdYear and ProdYear is not None and str(ProdYear) not in ['None', 'nan', '<NA>', 'null', '']:
                            if len(str(ProdYear))>0 and  '.0' in str(ProdYear): ProdYear = str(ProdYear).replace('.0','')
                        else:  ProdYear = ''

                        if Country == Country and Country is not None and len(Country)>0:  
                            sol = Country.strip()   
                            if len(sol)>2 and Country !='none': iso = getiso(sol)
                            elif len(sol)>2 and Country =='none': 
                                iso = ''
                                Country = ''
                            else: iso = sol
                        if '[' in iso: iso = ''

                        if MasterTitle == MasterTitle and MasterTitle is not None and str(MasterTitle) not in ['None', 'nan', '<NA>', 'null']:  
                            if len(MasterTitle)>0 and type(MasterTitle) != 'NoneType': MasterTitle= MasterTitle.replace("'", "").replace('"', "")
                            ft = MasterTitle
                        else: 
                            MasterTitle = ''


                        hashKey = 'nohash'
                        hashPRJKey = 'nohash'

                        hashs = getHash(MasterTitle, SeasonNum, EpisodeNum, Project, iso, ProdYear, IsSeries)

                        if len(MasterTitle)>0: hashKey = hashs[0]
                        if len(Project)>0: hashPRJKey = hashs[1]
                        if len(MasterTitle)>0 or len(Project)>0:
                            uploadtime = datetime.datetime.now(tz=None) ### - datetime.timedelta(days=300)

                        n+=1

                        sd = {
                            "hashKey" : hashKey, 
                            "hashPRJKey" : hashPRJKey,
                            "OriginalItemID":OriginalItemID,
                            "catalogid": catalogid,
                            "FullTitle": FullTitle,
                            "MasterTitle": MasterTitle,
                            "IsSeries" : IsSeries,
                            "Project": Project,
                            "SeasonNum": SeasonNum,
                            "EpisodeNum": EpisodeNum,
                            "ProdYear": ProdYear,
                            "ProdCountry": countryList(Country.strip()),
                            "isoCountry": iso, 
                            "uploadtime": uploadtime,
                            "editor": editor}

                        sds.append(sd)

                        if n%m == 0:  
                            inCat(sds) 
                            sds = []
                    
                if len(sds) > 0: 
                    inCat(sds) 
            
            return int(total)
        
        def ContactB(parn):
            logs = []
            for p in parn:
                catID = part[p]['cat'] 
                for i in range(0,1):
                    z = mainContacts(dayup, catID) 
                    log = {
                        'editor' :'Contacts',
                        'partner_name' : p,
                        'catalogid' : catID,
                        'count_OriginalItemID': z,
                        'uploadtime': datetime.datetime.now(tz=None)
                    }
                    logs.append(log)
                    if z <10000: break
                        
            if len(logs)>0: inLog(logs) 
                
            ## Up CatHash 
            db['Contacts'].drop_index("hash")
            db['Contacts'].create_index([("hash", pymongo.DESCENDING)],  background=True, name="hash") 

        logger.info(f'Готовим UnOfficial_KP') 
        def GetKP():  
            q = f"with \
            KP AS ( SELECT  cu_id, MAX(upload_datetime) as uploadtime FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table4')} group by  cu_id ) \
            SELECT DISTINCT nameRu AS Project,  `year` AS ProdYear,  countries AS Country, serial AS IsSeries, cu_id AS OriginalItemID, uploadtime \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table4')} AS DD \
            JOIN KP ON KP.cu_id = DD.cu_id and KP.uploadtime = DD.upload_datetime " 
            lst = client.execute(q)  
            dfp = pd.DataFrame(lst, columns=["Project", "ProdYear", "Country", "IsSeries", "OriginalItemID", "uploadtime"])
            return dfp

        def mainKP():
            
            dfp = GetKP() 
            total = len(dfp) 
            n = 0
            sds = [] 
            hashCO = []
            uploadtime = datetime.datetime.now(tz=None) + datetime.timedelta(hours=1) 
            m = 10000 
            if total>0:
                for index, row in dfp.iterrows(): 
                    Project = str(row['Project'])
                    FullTitle = str(row['Project'])
                    ProdYear = str(row['ProdYear']) 
                    Country = str(row['Country']).lower() 
                    IsSeries = row['IsSeries'] 
                    OriginalItemID = row['OriginalItemID']  
                    catalogid = 119 
                    iso = ''
                    PRJCountry = ''
                    ft = '' 
                    iss = ''
                    hashPRJKey = 'nohash'
                    hashKey = 'nohash'
                    MasterTitle = str(row['Project']) 
                    SeasonNum = ''
                    EpisodeNum = '' 
                    
                    if ProdYear == ProdYear and ProdYear is not None and str(ProdYear) not in ['None', 'nan', '<NA>', 'null', '']:
                        if len(str(ProdYear))>0 and  '.0' in str(ProdYear): ProdYear = str(ProdYear).replace('.0','')
                    else:  ProdYear = ''
                    
                    if Country == Country and Country is not None and len(Country)>0:  
                        sol = Country.strip()   
                        if len(sol)>2 and Country !='none': iso = getiso(sol)
                        elif len(sol)>2 and Country =='none': 
                            iso = ''
                            Country = ''
                        else: iso = sol
                    if '[' in iso: iso = ''
                    
                    if MasterTitle == MasterTitle and MasterTitle is not None and str(MasterTitle) not in ['None', 'nan', '<NA>', 'null']:  
                        if len(MasterTitle)>0 and type(MasterTitle) != 'NoneType': MasterTitle= MasterTitle.replace("'", "").replace('"', "")
                        ft = MasterTitle
                    else: 
                        MasterTitle = '' 
                    
                    hashs = getHash(MasterTitle, SeasonNum, EpisodeNum, Project, iso, ProdYear, IsSeries)
                    
                    if len(MasterTitle)>0: hashKey = hashs[0]
                    if len(Project)>0: hashPRJKey = hashs[1]
                        
                    n+=1
                            
                    if hashKey != 'nohash' and hashPRJKey != 'nohash':
                    
                        sd = {
                            "hashKey" : hashKey, 
                            "hashPRJKey" : hashPRJKey,
                            "OriginalItemID":OriginalItemID,
                            "catalogid": catalogid,
                            "FullTitle": FullTitle,
                            "MasterTitle": MasterTitle,
                            "IsSeries" : IsSeries,
                            "Project": Project,
                            "SeasonNum": SeasonNum,
                            "EpisodeNum": EpisodeNum,
                            "ProdYear": ProdYear,
                            "ProdCountry": countryList(Country.strip()),
                            "isoCountry": iso, 
                            "uploadtime": uploadtime, 
                            "editor": 'UnOfficial_KP'} 
                        sds.append(sd)
                            
                        if n%m == 0:  
                            inCat(sds) 
                            sds = []
                    
                if len(sds) > 0: 
                    inCat(sds) 
            
            return int(total)

        def UnKP():
            logs =[]
            z = mainKP()
            log = {
                        'editor' :'UnOfficial_KP',
                        'partner_name' : 'kinopoisk',
                        'catalogid' : 119,
                        'count_OriginalItemID': z,
                        'uploadtime': datetime.datetime.now(tz=None)
                    }
            logs.append(log)
            if len(logs)>0: inLog(logs) 

        logger.info(f'Готовим данные из каталога  (без проверки на уникальность)')
        def GetCat(catalogid): 
            tdays = datetime.datetime.now(tz=None) - datetime.timedelta(days=3)
            tdays = tdays.strftime('%Y-%m-%d')
            q = f"WITH FA AS ( SELECT DISTINCT OriginalItemID, CatalogID, MAX(LastUpdateDate) AS uploadtime \
            FROM {self.args.get('Output.ClickHouse.schema2') + '.' + self.args.get('Output.ClickHouse.table2')} \
            WHERE CatalogID = "+str(catalogid)+"  AND OriginalItemID NOT LIKE 'adv_%' \
            AND LastUpdateDate > '"+str(tdays)+f"' \
            GROUP BY OriginalItemID, CatalogID) \
            SELECT DISTINCT OriginalItemID, CatalogID, FullTitle, Title, IsSeries, if(notEmpty(Project), Project, '') Project, SeasonNum,   EpisodeNum , ProdYear, ProdCountry, Duration, ReleaseDate \
            FROM {self.args.get('Output.ClickHouse.schema2') + '.' + self.args.get('Output.ClickHouse.table2')} AS A \
            JOIN FA ON A.OriginalItemID = FA.OriginalItemID AND A.CatalogID = FA.CatalogID AND A.LastUpdateDate = FA.uploadtime \
            WHERE CatalogID = "+str(catalogid)+" "
            
            lst = client.execute(q)  
            dfp = pd.DataFrame(lst, columns=["OriginalItemID", "CatalogID", "FullTitle", "Title", "IsSeries", "Project", "SeasonNum", "EpisodeNum", "ProdYear", "ProdCountry", "Duration", "ReleaseDate"])
            return dfp


        def mainCat(catalogid):
            start = datetime.datetime.now(tz=None)   
            dfp = GetCat(catalogid)  
            total = len(dfp) 
            n = 0
            sds = [] 
            hashCO = []
            uploadtime = datetime.datetime.now(tz=None) 
            m = 10000 
            
            if total>0:
                for index, row in dfp.iterrows():
                    
                    cuhash = hashlib.md5(str(row).encode('utf-8')).hexdigest()
                    
                    if mgetCatHahs(cuhash) == False:
                    
                        OriginalItemID = row['OriginalItemID'] 
                        iso = ''
                        PRJCountry = ''
                        ft = ''
                        SeasonNum = ''
                        EpisodeNum = ''
                        ProdYear = ''
                        iss = ''
                        MasterTitle = row['Title'] 
                        IsSeries = row['IsSeries'] 
                        FullTitle = row['FullTitle'] 
                        Project = row['Project'].replace('"','').capitalize()
                        SeasonNum = row['SeasonNum'] 
                        EpisodeNum = row['EpisodeNum'] 
                        ProdYear = row['ProdYear'] 
                        ReleaseDate = row['ReleaseDate']
                        Duration = row['Duration']
                        Country = str(row['ProdCountry']).lower() 


                        if FullTitle != FullTitle or FullTitle is None or str(FullTitle) in ['None', 'nan', '<NA>', 'null']: FullTitle = ''
                        else:  FullTitle = FullTitle.replace("'", "").replace('"', "") 
                        
                        if SeasonNum == SeasonNum  and SeasonNum is not None and str(SeasonNum) not in ['None', 'nan', '<NA>', 'null']:
                            if len(str(SeasonNum))>0 and str(SeasonNum).replace('.0','') != '0':
                                SeasonNum = str(SeasonNum).replace('.0','')
                                IsSeries = True
                            else: SeasonNum = ''
                        else: SeasonNum = ''
        
                        if EpisodeNum == EpisodeNum and len(str(EpisodeNum))>0 and EpisodeNum is not None and str(EpisodeNum) not in ['None', 'nan', '<NA>', 'null']:
                            if len(str(EpisodeNum))>0 and str(EpisodeNum).replace('.0','') != '0':
                                EpisodeNum = str(EpisodeNum).replace('.0','')
                                IsSeries = True
                            else: EpisodeNum = ''
                        else: EpisodeNum = ''


                        if ProdYear == ProdYear and ProdYear is not None and str(ProdYear) not in ['None', 'nan', '<NA>', 'null', '']:
                            if len(str(ProdYear))>0 and  '.0' in str(ProdYear): ProdYear = str(ProdYear).replace('.0','')
                        else:  ProdYear = ''

                        if Country == Country and Country is not None and len(Country)>0:  
                            sol = Country.strip()   
                            if len(sol)>2 and Country !='none': iso = getiso(sol)
                            elif len(sol)>2 and Country =='none': 
                                iso = ''
                                Country = ''
                            else: iso = sol
                        if '[' in iso: iso = ''

                        if MasterTitle == MasterTitle and MasterTitle is not None and str(MasterTitle) not in ['None', 'nan', '<NA>', 'null']:  
                            if len(MasterTitle)>0 and type(MasterTitle) != 'NoneType': MasterTitle= MasterTitle.replace("'", "").replace('"', "")
                            ft = MasterTitle
                        else: 
                            MasterTitle = ''

                        if catalogid == 124:
                            if 'e' in MasterTitle.lower() and 's' in MasterTitle.lower():
                                for qq in MasterTitle.lower().split('e'):
                                    if 's' in qq:
                                        SeasonNum = str(int(qq.replace('s','')))
                                    else: 
                                        if re.search(r'[0-9]', EpisodeNum) is not None: EpisodeNum = str(int(qq))
                                        else: EpisodeNum = str(qq)


                        hashs = getHash(MasterTitle, SeasonNum, EpisodeNum, Project, iso, ProdYear, IsSeries)
                        
                        hashKey =  hashs[0]
                        hashPRJKey = hashs[1]
                        n+=1
                    
                        sd = {
                            "IsMaster" : False,
                            "hashKey" : hashKey, 
                            "hashPRJKey" : hashPRJKey,
                            "OriginalItemID":OriginalItemID,
                            "catalogid": catalogid,
                            "FullTitle": FullTitle,
                            "MasterTitle": MasterTitle,
                            "IsSeries" : IsSeries,
                            "Project": Project,
                            "SeasonNum": SeasonNum,
                            "EpisodeNum": EpisodeNum,
                            "ProdYear": str(ProdYear),
                            "ProdCountry": Country.strip(),
                            "isoCountry": iso,
                            "uploadtime": uploadtime,
                            "editor": 'Catalog',
                            "IsTrailer" : False,
                            "Duration": Duration,
                            "ReleaseDate": ReleaseDate
                            }
        
                        if catalogid == 119:
                            if hashPRJKey != 'nohash': 
                                sds.append(sd)  
                        else:
                            sds.append(sd) 
                        
                        if n%m == 0:
                            inCatz(sds) 
                            sds = []
                        
                    
                if len(sds) > 0: 
                    inCatz(sds)  
                    
            return int(total)
            
        def UpCats():
            logs =[]

            for p in parn:
                catID = part[p]['cat'] 
                z = mainCat(catID) 
                log = {
                        'editor' :'Catalog',
                        'partner_name' : p,
                        'catalogid' : catID,
                        'count_OriginalItemID': z,
                        'uploadtime': datetime.datetime.now(tz=None)
                }
                ## Up CatHash 
                db['CatalogHath'].drop_index("CatHash")
                db['CatalogHath'].create_index([("CatHash", pymongo.DESCENDING)],  background=True, name="CatHash")
                logs.append(log)
            if len(logs)>0: inLog(logs)	

        logger.info('Готовим ISO Update')
        def getNotISO(prj, pr, catid): 
            qсp = f"with \
            NU AS ( \
            SELECT OriginalItemID, MAX(uploadtime) as uploadtime FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} where  catalogid = "+str(catid)+f" group by OriginalItemID ) \
            SELECT OriginalItemID, Project, ProdYear \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} as NC \
            JOIN NU on NU.OriginalItemID = NC.OriginalItemID AND NU.uploadtime = NC.uploadtime \
            where  catalogid = "+str(catid)+" and Project != '' and isoCountry = '' \
            ORDER by Project, ProdYear" 
        
            
            lstp = client.execute(qсp)
            dfp = pd.DataFrame(lstp, columns=["OriginalItemID","Project", "ProdYear"])
            n = 0
            for k, v in dfp.iterrows():
                Project = v['Project']  
                ProdYears = v['ProdYear']  
                
                if Project not in prj:
                    prj.append(Project)
                    pr.update({Project:ProdYears})

        prj = []
        pr = {} 
        

        def getONEiso(nn, Project, ProdYear, catid):   
            qсp = f"with \
            NU AS ( \
            SELECT OriginalItemID, MAX(uploadtime) as uploadtime FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} where  catalogid != "+str(catid)+f" group by OriginalItemID ) \
            SELECT distinct isoCountry AS isoCountrys \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} as NC \
            JOIN NU on NU.OriginalItemID = NC.OriginalItemID AND NU.uploadtime = NC.uploadtime \
            where  lowerUTF8(Project) = '"+Project.lower()+"' and ProdYear = '"+ProdYear+"'" 
        
            lstp = client.execute(qсp)
            dfp = pd.DataFrame(lstp, columns=["isoCountrys"]) 
            
            if len(dfp['isoCountrys']) ==1:
                for k, v in dfp.iterrows():
                    iso = v['isoCountrys'] 
                    ProdCountry = CoISO(iso)
                    if ProdCountry != False: 
                        isomain(ProdYear, Project, ProdCountry, iso, catid)
                return True

        def isomain(_ProdYear, Project, Country, iso, catid):
            q = f"with \
            NU AS ( \
            SELECT OriginalItemID, MAX(uploadtime) as uploadtime FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} group by OriginalItemID ) \
            SELECT distinct IsMaster, OriginalItemID, FullTitle, MasterTitle, Project, SeasonNum, EpisodeNum, ProdYear,  IsSeries, uploadtime, IsTrailer, Duration, ReleaseDate \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} as NC \
            JOIN NU on NU.OriginalItemID = NC.OriginalItemID AND NU.uploadtime = NC.uploadtime \
            where lowerUTF8(Project) = '"+Project.lower()+"' and ProdYear = '"+_ProdYear+"' and isoCountry = '' " 
            
            lst = client.execute(q)
            dfp = pd.DataFrame(lst, columns=["IsMaster",  "OriginalItemID", "FullTitle", "MasterTitle", "Project", "SeasonNum", "EpisodeNum", "ProdYear",  "IsSeries", "uploadtime", "IsTrailer", "Duration", "ReleaseDate"])
            
            
            if len(iso) !=2:

                if Country == Country and Country is not None and len(Country)>0:  
                    sol = Country.strip()   
                    if len(sol)>2 and Country !='none': iso = getiso(sol)
                    elif len(sol)>2 and Country =='none': 
                        iso = ''
                        Country = ''
                    elif len(sol) ==2: iso = sol
                else: iso = ''
                
            if '[' in iso: iso = ''
                        
            
            total = len(dfp) 
            n = 0
            sds = [] 
            hashCO = [] 
            if total>0:   
                for index, row in dfp.iterrows():   
                    IsMaster = row['IsMaster']  
                    IsTrailer = row['IsTrailer']  
                    OriginalItemID = row['OriginalItemID'] 
                    catalogid = catid
                    FullTitle = row['FullTitle'] 
                    MasterTitle = row['MasterTitle'] 
                    IsSeries = row['IsSeries']  
                    Project = row['Project'] 
                    SeasonNum = row['SeasonNum'] 
                    EpisodeNum = row['EpisodeNum']
                    ProdYear = row['ProdYear'] 
                    Duration = row['Duration'] 
                    ReleaseDate = row['ReleaseDate'] 
                    uploadtime =  row['uploadtime'] + datetime.timedelta(hours=1)
                    
                    n+=1  
                    
                    hashs = getHash(MasterTitle, SeasonNum, EpisodeNum, Project, iso, ProdYear, IsSeries)
                    hashKey = hashs[0]
                    hashPRJKey = hashs[1]
                    sd = {
                        "IsMaster" : IsMaster,
                        "hashKey" : hashKey, 
                        "hashPRJKey" : hashPRJKey,
                        "OriginalItemID": OriginalItemID,
                        "catalogid": catalogid,
                        "FullTitle": FullTitle,
                        "MasterTitle": MasterTitle,
                        "IsSeries" : IsSeries,
                        "Project": Project,
                        "SeasonNum": SeasonNum,
                        "EpisodeNum": EpisodeNum,
                        "ProdYear": ProdYear,
                        "ProdCountry": Country,
                        "isoCountry": iso,
                        "uploadtime": uploadtime,
                        "editor": 'UpCountry',
                        "IsTrailer": IsTrailer,
                        "Duration": Duration,
                        "ReleaseDate" : ReleaseDate
                    }
                    
                    CO = str(row['OriginalItemID'])
                    hCO = hashlib.md5(CO.encode('utf-8')).hexdigest() 
                    
                    if hCO not in hashCO:
                        sds.append(sd)
                        hashCO.append(hCO)  
        
                if len(sds) > 0: 
                    inCatz(sds) 
                    
        def mgetISOHahs(h):
            co = db['RawISO']
            x = False
            result = co.find_one({"hash": h, "stat": {'$gt': 3} })
            if result: x = True
            else:
                co.update_one({"hash": h}, {'$set': {'day': datetime.datetime.now(tz=None)}, '$inc': {'stat': 1} }, upsert=True)
            return x            
                    
        def isoPRJs(catid):
            
            getNotISO(prj, pr, catid)   

            nn = 0
            qids = 0
            logs = []

            for i in prj:
                nn +=1
                Project = i
                ProdYear = pr[i]
                coH = str(catid)+'-'+pr[i]+'-'+Project
                if mgetISOHahs(coH) == False:
                    za = getONEiso(nn, Project, ProdYear, catid)
                    if za == True: qids +=1
            
            log = {
            'editor' :'UpCountry',
            'partner_name' : 'start',
            'catalogid' : 116,
            'count_OriginalItemID': qids,
            'uploadtime': datetime.datetime.now(tz=None)
            }
            logs.append(log)   
            
            if len(logs)>0: inLog(logs) 
            
        logger.info(f'Трейлеры:')
        def MakeTrailers(catalogid):  
            
            q = f"WITH \
            N AS  ( SELECT Project, OriginalItemID, IsTrailer, MAX(uploadtime) AS updatedate FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} WHERE catalogid = "+str(catalogid)+f"  GROUP BY Project, OriginalItemID, IsTrailer ), \
            T AS  ( SELECT ContentUnitID, MAX(upload_datetime) AS uploaddatetime FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table7')} WHERE  IsTrailer = TRUE  and ContentUnitCatalogID = '"+str(catalogid)+f"' GROUP BY ContentUnitID), \
            TT AS ( SELECT ContentUnitID, IsTrailer FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table7')} AS TM JOIN T ON T.ContentUnitID = TM.ContentUnitID AND T.uploaddatetime = TM.upload_datetime WHERE  IsTrailer = TRUE  and ContentUnitCatalogID = '"+str(catalogid)+f"' GROUP BY ContentUnitID, IsTrailer) \
            SELECT distinct TT.IsTrailer, NC.* \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} AS NC JOIN N ON N.OriginalItemID = NC.OriginalItemID AND N.updatedate = NC.uploadtime JOIN TT ON NC.OriginalItemID = TT.ContentUnitID \
            WHERE NC.catalogid = "+str(catalogid)+" and TT.IsTrailer = TRUE  \
            SETTINGS distributed_product_mode = 'global'" 
            
            lst = client.execute(q)
            dfp = pd.DataFrame(lst, columns=['TT.IsTrailer', 'NC.IsMaster', 'NC.hashKey', 'NC.hashPRJKey', 'NC.OriginalItemID', 'NC.catalogid', 'NC.FullTitle', 'NC.MasterTitle', 'NC.IsSeries', 'NC.Project', 'NC.SeasonNum', 'NC.EpisodeNum', 'NC.ProdYear', 'NC.ProdCountry', 'NC.isoCountry', 'NC.uploadtime', 'NC.editor', 'NC.IsTrailer'])
            return dfp

        def MakeNewTrailers(catalogid):  
            
            q = f"WITH N AS  ( SELECT OriginalItemID, MAX(uploadtime) AS updatedate FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} WHERE catalogid = "+str(catalogid)+f"  GROUP BY OriginalItemID ) \
            SELECT IsMaster, hashKey, hashPRJKey, OriginalItemID, catalogid, FullTitle, MasterTitle, \
            IsSeries, Project, SeasonNum, EpisodeNum, ProdYear, ProdCountry, isoCountry, uploadtime, editor, IsTrailer \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} AS C \
            JOIN N ON C.OriginalItemID = N.OriginalItemID AND C.uploadtime = N.updatedate \
            where IsTrailer = TRUE" 
            
            lst = client.execute(q)
            dfp = pd.DataFrame(lst, columns=['IsMaster', 'hashKey', 'hashPRJKey', 'OriginalItemID', 'catalogid', 'FullTitle', 'MasterTitle', 'IsSeries', 'Project', 'SeasonNum', 'EpisodeNum', 'ProdYear', 'ProdCountry', 'isoCountry', 'uploadtime', 'editor', 'IsTrailer'])
            return dfp


        def MakeOnlyNewTrailers(catalogid):  
            
            upload_datetime = str(datetime.datetime.now(tz=None) - datetime.timedelta(days=2) )[:10]
            
            q = f"WITH \
            N AS  ( SELECT OriginalItemID, MAX(uploadtime) AS updatedate FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} WHERE catalogid = "+str(catalogid)+f"  GROUP BY OriginalItemID ), \
            T AS  ( SELECT ContentUnitID, MAX(upload_datetime) AS uploaddatetime FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table7')} WHERE  ContentUnitCatalogID = '"+str(catalogid)+f"' GROUP BY ContentUnitID), \
            TT AS ( SELECT ContentUnitID, IsTrailer, TM.upload_datetime FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table7')} AS TM JOIN T ON T.ContentUnitID = TM.ContentUnitID AND T.uploaddatetime = TM.upload_datetime \
            WHERE  ContentUnitCatalogID = '"+str(catalogid)+f"' GROUP BY ContentUnitID, IsTrailer, upload_datetime ) \
            SELECT distinct TT.IsTrailer, NC.* \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table1')} AS NC JOIN N ON N.OriginalItemID = NC.OriginalItemID AND N.updatedate = NC.uploadtime JOIN TT ON NC.OriginalItemID = TT.ContentUnitID \
            WHERE NC.catalogid = "+str(catalogid)+"  \
            and editor NOT  IN ( 'marina.korovina', 'Elena.Radyukova', 'Marina.Korovina', 'Moderator1', 'Admin', 'admin' ) \
            and TT.upload_datetime > '"+str(upload_datetime)+"' SETTINGS distributed_product_mode = 'global'" 
            
            lst = client.execute(q)
            dfp = pd.DataFrame(lst, columns=['TT.IsTrailer', 'NC.IsMaster', 'NC.hashKey', 'NC.hashPRJKey', 'NC.OriginalItemID', 'NC.catalogid', 'NC.FullTitle', 'NC.MasterTitle', 'NC.IsSeries', 'NC.Project', 'NC.SeasonNum', 'NC.EpisodeNum', 'NC.ProdYear', 'NC.ProdCountry', 'NC.isoCountry', 'NC.uploadtime', 'NC.editor', 'NC.IsTrailer', 'NC.Duration', 'NC.ReleaseDate'])
            return dfp

        def trailer_main(catalogid):
            
            dfp = MakeOnlyNewTrailers(catalogid)
            
            total = len(dfp)
            n = 0
            sds = [] 
            hashCO = []
            m = 10000 
            if total>0:
                for index, row in dfp.iterrows():
                    
                    IsMaster= row['NC.IsMaster'] 
                    IsTrailer= row['NC.IsTrailer']
                    _IsTrailer= row['TT.IsTrailer']
                    hashKey = row['NC.hashKey']
                    hashPRJKey = row['NC.hashPRJKey'] 
                    OriginalItemID = row['NC.OriginalItemID']  
                    iso = row['NC.isoCountry'] 
                    MasterTitle = row['NC.MasterTitle'] 
                    IsSeries = row['NC.IsSeries']
                    catalogid = row['NC.catalogid'] 
                    FullTitle = row['NC.FullTitle'] 
                    Project = row['NC.Project'] 
                    SeasonNum = row['NC.SeasonNum'] 
                    EpisodeNum = row['NC.EpisodeNum'] 
                    ProdYear = row['NC.ProdYear'] 
                    ProdCountry = row['NC.ProdCountry'] 
                    Duration = row['NC.Duration']
                    ReleaseDate = row['NC.ReleaseDate']
                    _uploadtime = row['NC.uploadtime']           
                    uploadtime = _uploadtime + datetime.timedelta(hours=1)
                        
                    
                    if IsTrailer != _IsTrailer: 
                        n+=1
                        sd = {
                            "IsMaster" : IsMaster,
                            "IsTrailer" : _IsTrailer,
                            "hashKey" : hashKey, 
                            "hashPRJKey" : hashPRJKey,
                            "OriginalItemID":OriginalItemID,
                            "catalogid": catalogid,
                            "FullTitle": FullTitle,
                            "MasterTitle": MasterTitle,
                            "IsSeries" : IsSeries,
                            "Project": Project,
                            "SeasonNum": SeasonNum,
                            "EpisodeNum": EpisodeNum,
                            "ProdYear": ProdYear,
                            "ProdCountry": ProdCountry,
                            "isoCountry": iso,
                            "uploadtime": uploadtime,
                            "Duration": Duration, 
                            "ReleaseDate" :ReleaseDate,
                            "editor": 'Trailers' }
        
                        sds.append(sd) 
                    
                    if n%m == 0:  
                        if len(sds)>0: 
                            inCatz(sds) 
                            sds = []
                        
                    
                    
                if len(sds) > 0: 
                    inCatz(sds) 
                    
            return int(total)

        def UpTrailers():
            for p in parn:
                catID = part[p]['cat'] 
                trailer_main(catID)	
                
        def get_fof():  
     
            q = f"SELECT  IsMaster, hashKey, hashPRJKey, OriginalItemID, catalogid, FullTitle, MasterTitle, IsSeries, \
            Project, SeasonNum, EpisodeNum, ProdYear, ProdCountry, isoCountry, uploadtime, editor, IsTrailer, Duration, ReleaseDate \
            FROM {self.args.get('Output.ClickHouse.schema1') + '.' + self.args.get('Output.ClickHouse.table8')} \
            where ( lowerUTF8(FullTitle) like '%фоф%' OR lowerUTF8(FullTitle) like '%о фильме%' \
            OR lowerUTF8(MasterTitle) like '%фоф%' OR lowerUTF8(MasterTitle) like '%о фильме%' ) \
            AND IsSeries = TRUE" 
              
            lst = client.execute(q) 
            dfp = pd.DataFrame(lst, columns=['IsMaster', 'hashKey', 'hashPRJKey', 'OriginalItemID', 'catalogid', 'FullTitle', 'MasterTitle', 'IsSeries', 'Project', 'SeasonNum', 'EpisodeNum', 'ProdYear', 'ProdCountry', 'isoCountry', 'uploadtime', 'editor', 'IsTrailer', 'Duration', 'ReleaseDate'])
            return dfp

        def fof(): 
            dfp = get_fof() 
            total = len(dfp) 
            sds = []  
            if total>0:
                for index, row in dfp.iterrows():
                    
                    IsMaster= row['IsMaster'] 
                    IsTrailer= row['IsTrailer'] 
                    hashKey = row['hashKey']
                    hashPRJKey = row['hashPRJKey'] 
                    OriginalItemID = row['OriginalItemID']  
                    iso = row['isoCountry'] 
                    MasterTitle = row['MasterTitle'] 
                    IsSeries = False
                    catalogid = row['catalogid'] 
                    FullTitle = row['FullTitle'] 
                    Project = row['Project'] 
                    SeasonNum = row['SeasonNum'] 
                    EpisodeNum = row['EpisodeNum'] 
                    ProdYear = row['ProdYear'] 
                    ProdCountry = row['ProdCountry'] 
                    Duration = row['Duration']
                    ReleaseDate = row['ReleaseDate']
                    _uploadtime = row['uploadtime'] 
                    
                    uploadtime = _uploadtime + datetime.timedelta(hours=1)
                     
                    sd = {
                            "IsMaster" : IsMaster,
                            "IsTrailer" : IsTrailer,
                            "hashKey" : hashKey, 
                            "hashPRJKey" : hashPRJKey,
                            "OriginalItemID":OriginalItemID,
                            "catalogid": catalogid,
                            "FullTitle": FullTitle,
                            "MasterTitle": MasterTitle,
                            "IsSeries" : IsSeries,
                            "Project": Project,
                            "SeasonNum": SeasonNum,
                            "EpisodeNum": EpisodeNum,
                            "ProdYear": ProdYear,
                            "ProdCountry": ProdCountry,
                            "isoCountry": iso,
                            "uploadtime": uploadtime,
                            "Duration": Duration, 
                            "ReleaseDate" :ReleaseDate,
                            "editor": 'Up FOF' }
        
                    sds.append(sd) 
                      
                if len(sds) > 0: inCatz(sds)  


        logger.info(f'Запускаем обновление')		

        logger.info(f"Ежедневное обновление контактов для: '1tv', 'premier_one'")
        ContactA(['1tv', 'premier_one' ])	
            
        logger.info(f"Ежедневное обновление контактов для остальных")
        ContactB([ 'ivi', 'ntv', '_ctc_tv', 'kinopoisk', 'rostelecom', 'tricolor', 'beeline', 'okko',  'kino.1tv', 'smotrim', 'mts', 'tnt_tv',  'start', 'amediateka'])	

        logger.info(f"Обновление из metrix.ABC_parsed_UnOfficial_KP")
        UnKP()

        logger.info(f"Обновление из каталогов")
        UpCats()


        logger.info(f"Обновление данных по трейлерам")
        UpTrailers()
        
        logger.info(f"Обновление стран для ряда партнеров")
        isoPRJs(110)
        isoPRJs(116)

        
        logger.info(f"Обновление данных по ФОФ")
        fof()

def main():
    transformation = query_CH_test(
        params=sys.argv[1:],
        required_params=REQUIRED_PARAMS,
        optional_params=OPTIONAL_PARAMS,
        dev_params=DEV_PARAMS,
    )
    transformation.run()

if __name__ == '__main__':
    main()

