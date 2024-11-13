### селект запросы для заданий из части SQL (там все данные за 2023 год, но я на всякий случай добавил фильтры по дате)
task1_query = """
                    with cte as (
                        SELECT 
                            t.store_id, 
                            count(*) as transactions_count
                        FROM 
                            transactions t 
                        WHERE t.transaction_date between '2023-01-01' and '2023-12-31'
                        GROUP BY 
                            t.store_id
                    )

                    SELECT 
                        s.store_id, 
                        s.city, 
                        s.state, 
                        cte.transactions_count
                    FROM 
                        stores s 
                    LEFT JOIN 
                        cte 
                    ON 
                        s.store_id = cte.store_id 
                    ORDER BY 
                        transactions_count DESC

"""

task2_query = """    
                    with cte as (
                        SELECT
                            t.customer_id,
                            count(*) as transactions_count
                        FROM 
                            transactions t
                        WHERE t.customer_id in (
                            SELECT 
                                c.customer_id
                            FROM
                            transactions t
                            LEFT JOIN 
                                customers c 
                            ON 
                                t.customer_id = c.customer_id 
                            WHERE 
                                c.signup_date between '2023-01-01' and '2023-12-31'
                                AND 
                                julianday(t.transaction_date) - julianday(c.signup_date) < 31
                        )
                        GROUP BY 
                            t.customer_id
                    )

                    SELECT 
                        c.customer_id, 
                        c.name, 
                        cte.transactions_count
                    FROM 
                        cte
                    LEFT JOIN 
                        customers c 
                    ON 
                        c.customer_id = cte.customer_id
"""



task3_query = """
                    SELECT 
                        t.category, 
                        sum(t.amount) as total_sales 
                    FROM 
                        transactions t 
                    WHERE 
                        t.transaction_date between '2023-01-01' and '2023-12-31'
                    GROUP BY 
                        t.category
                    ORDER BY 
                        total_sales DESC
                    LIMIT 3
"""