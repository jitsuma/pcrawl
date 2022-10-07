# pcrawl

Path example - C:\Users\Jimmy\Desktop\scraper2-new2\scraper2\scraper\scraper\spiders\pcrawl

1. Navigate to ```new_geo_expansion_export.py``` file in ```pcrawl``` folder.

2. Switch path of following lines to where 'pcrawl' folder is located on your device in order to find the 'apartments_filter_unique_zipcode.csv' file.

Change out for line 550
  os.remove('/Users/Jimmy/Desktop/scraper2-new2/scraper2/scraper/scraper/spiders/pcrawl/apartments_filter_unique_zipcode.csv')

Change our for line 574
  df_items = pd.read_csv('/Users/Jimmy/Desktop/scraper2-new2/scraper2/scraper/scraper/spiders/pcrawl/apartments_filter_unique_zipcode.csv',encoding='utf-8')
  
3. Repeat same steps above for ```test_geo_export.py``` file
