import luigi
import pandas as pd
from src.extract.extract_data import extract_marketing_data, extract_sales_data, extract_scraping_data
from src.validation.validate_data import validation_process
from src.transformation.transform_data import transform_sales_data, transform_marketing_data, transform_scraping_data
from src.load.load_data import load_sales_data, load_marketing_data, load_scraping_data
        
class ExtractSalesData(luigi.Task):

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("data/extract/extract_sales_data.csv")
                
    
    def run(self):
        extract_sales_data().to_csv(self.output().path,index=False)

class ExtractMarketingData(luigi.Task):

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("data/extract/extract_marketing_data.csv")
                
    
    def run(self):
        extract_marketing_data().to_csv(self.output().path,index=False)

class ExtractScrapingData(luigi.Task):

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("data/extract/extract_scraping_data.csv")
                
    
    def run(self):
        extract_scraping_data(pages=1).to_csv(self.output().path,index=False)

# class ValidateData(luigi.Task):

#     def requires(self):
#         return [ExtractSalesData(),
#                 ExtractMarketingData(),
#                 ExtractScrapingData()]
    
#     def output(self):
#         return luigi.LocalTarget('data/validate/validate_data.txt')

#     def run(self):
#         # read sales data
#         validate_sales_data = pd.read_csv(self.input()[0].path)

#         # read marketing data
#         validate_marketing_data = pd.read_csv(self.input()[1].path)

#         # read journal data
#         validate_scraping_data = pd.read_csv(self.input()[2].path)

#         # validate data
#         validation_process(df = validate_sales_data,
#                            table_name = "sales")
        
#         validation_process(df = validate_marketing_data,
#                            table_name = "marketing")
        
#         validation_process(df = validate_scraping_data,
#                            table_name = "scraping")

class TransformSalesData(luigi.Task):

    def requires(self):
        return ExtractSalesData()
    
    def output(self):
        return luigi.LocalTarget("data/transform/transform_sales_data.csv")
    
    def run(self):
        # read data from previous source
        sales_df = pd.read_csv(self.input().path)
        
        #Transform the data
        sales_df = transform_sales_data(sales_df)

        # save the output to csv
        sales_df.to_csv(self.output().path, index = False)

class TransformMarketingData(luigi.Task):

    def requires(self):
        return ExtractMarketingData()
    
    def output(self):
        return luigi.LocalTarget("data/transform/transform_marketing_data.csv")
    
    def run(self):
        # read data from previous source
        marketing_df = pd.read_csv(self.input().path)
        
        #Transform the data
        marketing_df = transform_marketing_data(marketing_df)

        # save the output to csv
        marketing_df.to_csv(self.output().path, index = False)

class TransformScrapingData(luigi.Task):

    def requires(self):
        return ExtractScrapingData()
    
    def output(self):
        return luigi.LocalTarget("data/transform/transform_scraping_data.csv")
    
    def run(self):
        # read data from previous source
        scraping_df = pd.read_csv(self.input().path)
        
        #Transform the data
        scraping_df = transform_scraping_data(scraping_df)

        # save the output to csv
        scraping_df.to_csv(self.output().path, index = False)
        

class LoadSalesData(luigi.Task):

    def requires(self):
        return TransformSalesData()
    
    def output(self):
        return luigi.LocalTarget("data/load/load_sales_data.csv")
    
    def run(self):
        # read data from previous task
        sales_df_transformed = pd.read_csv(self.input().path)

        # read data from previous task Load the data to the DWH
        load_sales_data(pd.read_csv(self.input().path))

        # save the output
        sales_df_transformed.to_csv(self.output().path, index = False)
        

class LoadMarketingData(luigi.Task):

    def requires(self):
        return TransformMarketingData()
    
    def output(self):
        return luigi.LocalTarget("data/load/load_marketing_data.csv")
    
    def run(self):
        # read data from previous task
        marketing_df_transformed = pd.read_csv(self.input().path)

        # Load the data to the DWH
        load_marketing_data(marketing_df_transformed)

        # save the output
        marketing_df_transformed.to_csv(self.output().path, index = False)

class LoadScrapingData(luigi.Task):

    def requires(self):
        return TransformScrapingData()
    
    def output(self):
        return luigi.LocalTarget("data/load/load_scraping_data.csv")
    
    def run(self):
        # read data from previous task
        scraping_df_transformed = pd.read_csv(self.input().path)

        # Load the data to the DWH
        load_scraping_data(scraping_df_transformed)

        # save the output
        scraping_df_transformed.to_csv(self.output().path, index = False)

if __name__ == "__main__":
    
    luigi.build(
                [
                    ExtractSalesData(),
                    TransformSalesData(),
                    LoadSalesData(),
                    ExtractMarketingData(),
                    TransformMarketingData(),
                    LoadMarketingData(),
                    ExtractScrapingData(),
                    TransformScrapingData(),
                    LoadScrapingData(),
                    # ValidateData(),

                ]
                )