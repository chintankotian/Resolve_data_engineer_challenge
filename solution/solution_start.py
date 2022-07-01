import argparse
import pandas as pd
import  json
import os 

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


def load_transactions_files(transaction_path):
    if(transaction_path is not None):
        print("Reading transaction file from - "+transaction_path)
        transaction_result = pd.DataFrame([])
        counter = 0
        with open(transaction_path,"r") as file:

            while(True):    
                line = file.readline()
                if(not line):
                    break
                line = json.loads(line)
                single_transaction = pd.json_normalize(line,'basket',['customer_id','date_of_purchase'])
                transaction_result = pd.concat([transaction_result,single_transaction],ignore_index = True)
        return transaction_result

    else:
        raise NameError("Transaction Path is required and cannot be null")

def load_transactions(transaction_file_name = 'transactions.json',transaction_path = get_params()['transactions_location']):
    print("Loading all transactions from - "+transaction_path)
    try:
        transaction_df = pd.DataFrame([])
        for transaction_day in os.listdir(transaction_path): 
            final_transaction_path = f"{transaction_path}{transaction_day}/{transaction_file_name}"
            transaction_df = pd.concat([transaction_df,load_transactions_files(final_transaction_path)])
        
        return transaction_df

    except Exception as e:
        print("Couldn't load transactions from "+transaction_path)
        raise e



def load_csv_files(csv_path):    
    try:
        print("Reading csv file from - "+csv_path)
        if(csv_path is not None):
            return pd.read_csv(csv_path)
        else:
            raise ValueError("csv_path cannot be None")
    except Exception as e:
        print("Couldn't load data from csv path - "+str(csv_path))
        raise 



def write_json_file(df,file_name = 'output.json',csv_folder = get_params()['output_location']):
    try:

        if(df is not None or  df.count() != 0):
            output_file_path = f"{csv_folder}{file_name}"
            print("writing data to json path - "+output_file_path)
            if(not os.path.exists(csv_folder)):
                print(f"Output folder -  {csv_folder} doesn't exist")
                print(f"Created folder -  {csv_folder}")
                os.makedirs(csv_folder)

            if(os.path.exists(output_file_path)):
                print(f"Output file already esists , deleting existing file at - {output_file_path}")
                os.remove(output_file_path)

            df.to_json(output_file_path,orient ='records',indent = 2)
        else:
            raise ValueError("Source DF cannot None or empty")
    except Exception as e:
        print("Couldn't write data to csv path - "+str(f"{csv_folder}/{file_name}"))
        raise e

def main():
    print("Started Pipeline")
    params = get_params()

    # Load transactions
    tran_df = load_transactions()

    # Load customer and products data 
    customer_df = load_csv_files(params['customers_location']).astype({'customer_id':'string'})
    product_df = load_csv_files(params['products_location']).astype({'product_id':str})

    # Transaform transactio data to get purchase count
    transaction_count_df = tran_df.groupby(["customer_id","product_id"]).size().reset_index(name = 'purchase_count')
    transaction_count_df =  transaction_count_df.astype({'product_id':'string', 'customer_id':'string'})

    # Join dimension table to get descriptive fields
    final_df = transaction_count_df.join(customer_df.set_index('customer_id'),on = 'customer_id').join(product_df.set_index("product_id"), on = 'product_id')
    


    # Write final data to json 
    write_json_file(final_df[['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count']])
    
    print("Transaction pipeline completed")

if __name__ == "__main__":
    main()
