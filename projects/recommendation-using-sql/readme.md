Task:
In SQL implement a film recommendation system (show top 5 film recommendations for a specific customer) based on film rental data.

Description:
There is the following simplified data model of the film rental service:

         [film]   --   [shop_film]
                           |
                           |
                        [rental]   --   [customer]

I decided to use the following algorithm:
1. Get all films for a specific customer.
2. Identify all customers (except the specific customer) who have watched the same films.
3. Count the number of rentals by other customers except for films viewed by the specific customer.

Environment:
- Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 on VirtualBox