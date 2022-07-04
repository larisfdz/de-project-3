**The project is aimed to create and fullfill two database schemes, staging and mart.**

**dag_migration.py**
- Creates two schemes, staging and mart.  
- Updates staging.customer_orders_log table to add status field.  
- Creates a customer_retention data mart.
- Uploads historical data.


**etl_update_user_data_project.py**
- Gets yesterday's increment files.  
- Deletes the rows corresponding to the date if the above mentioned date already exists.    
- Ulploads staging scheme tables with the data from yesterday.  
- Checks the rows were added.  
- Updates mart dimention and facts tables.



