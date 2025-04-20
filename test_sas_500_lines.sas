/*********************************************
* TEST SAS CODE SAMPLE - 500 LINES
* This file contains various SAS programming patterns
* to properly test conversion capabilities
*********************************************/

/* Set libname for data storage */
LIBNAME sasdata "/path/to/sasdata";

/* Global macro variables for configuration */
%LET reporting_date = %SYSFUNC(TODAY());
%LET reporting_year = %SYSFUNC(YEAR(&reporting_date));
%LET reporting_month = %SYSFUNC(MONTH(&reporting_date));
%LET analysis_start_date = "01JAN&reporting_year"d;
%LET keep_vars = customer_id account_id transaction_date amount balance status;

/* Macro for date formatting */
%MACRO format_date(date=, format=YYMMDD10.);
    %IF %SYSFUNC(MISSING(&date)) %THEN %DO;
        .
    %END;
    %ELSE %DO;
        %SYSFUNC(PUTN(&date, &format))
    %END;
%MEND format_date;

/* Macro for standard data transformations */
%MACRO standard_transformations(input_ds=, output_ds=);
    DATA &output_ds;
        SET &input_ds;
        
        /* Date transformations */
        IF NOT MISSING(transaction_date) THEN DO;
            transaction_year = YEAR(transaction_date);
            transaction_month = MONTH(transaction_date);
            transaction_day = DAY(transaction_date);
            days_since_transaction = INTCK('DAY', transaction_date, &reporting_date);
        END;
        
        /* Numeric transformations */
        IF amount > 0 THEN positive_amount = amount;
        ELSE positive_amount = 0;
        
        IF amount < 0 THEN negative_amount = ABS(amount);
        ELSE negative_amount = 0;
        
        /* Calculate running balance */
        running_balance + amount;
        
        /* String transformations */
        customer_id_formatted = CATS('CUST_', PUT(customer_id, 8.));
        account_id_formatted = CATS('ACCT_', PUT(account_id, 10.));
        
        /* Flag calculations */
        large_transaction_flag = (ABS(amount) > 10000);
        high_balance_flag = (balance > 50000);
        
        /* Complex transformation with IFN */
        status_code = IFN(UPCASE(status) = 'ACTIVE', 1,
                        IFN(UPCASE(status) = 'PENDING', 2,
                          IFN(UPCASE(status) = 'CLOSED', 3, 99)));
    RUN;
    
    /* Add standard formats */
    PROC FORMAT;
        VALUE status_fmt
            1 = 'Active'
            2 = 'Pending'
            3 = 'Closed'
            99 = 'Unknown';
        
        VALUE flag_fmt
            0 = 'No'
            1 = 'Yes';
    RUN;
    
    /* Apply formats */
    DATA &output_ds;
        SET &output_ds;
        FORMAT status_code status_fmt.
               large_transaction_flag high_balance_flag flag_fmt.;
    RUN;
%MEND standard_transformations;

/* Import raw transaction data */
DATA sasdata.raw_transactions;
    INFILE '/path/to/transactions.csv' DLM=',' FIRSTOBS=2;
    INPUT 
        customer_id
        account_id
        transaction_date ANYDTDTE.
        transaction_type $
        amount
        balance
        status $
        description $100.
    ;
    
    /* Basic data cleaning */
    IF amount = . THEN DELETE;
    IF MISSING(transaction_date) THEN DELETE;
    
    /* Trim whitespace from string fields */
    transaction_type = STRIP(transaction_type);
    status = STRIP(status);
    description = STRIP(description);
    
    /* Standardize case */
    transaction_type = UPCASE(transaction_type);
    status = UPCASE(status);
RUN;

/* Create customer reference data */
DATA sasdata.customers;
    INPUT 
        customer_id
        first_name $
        last_name $
        join_date ANYDTDTE.
        customer_type $
        credit_score
    ;
    DATALINES;
1 John Smith 01JAN2015 REGULAR 720
2 Jane Doe 15FEB2016 PREMIUM 780
3 Robert Johnson 10MAR2017 REGULAR 690
4 Sarah Williams 20APR2018 PREMIUM 810
5 Michael Brown 05MAY2019 REGULAR 700
6 Jennifer Davis 12JUN2020 REGULAR 720
7 David Miller 25JUL2021 PREMIUM 750
8 Lisa Wilson 30AUG2018 REGULAR 680
9 Richard Moore 15SEP2019 PREMIUM 790
10 Patricia Taylor 28OCT2020 REGULAR 710
;
RUN;

/* Create account reference data */
DATA sasdata.accounts;
    INPUT 
        account_id
        customer_id
        account_type $
        open_date ANYDTDTE.
        close_date ANYDTDTE.
        initial_balance
    ;
    DATALINES;
101 1 CHECKING 01FEB2015 . 1500
102 1 SAVINGS 01FEB2015 . 5000
103 2 CHECKING 20FEB2016 . 2500
104 2 SAVINGS 20FEB2016 . 10000
105 3 CHECKING 15MAR2017 . 1000
106 4 CHECKING 25APR2018 . 2000
107 4 SAVINGS 25APR2018 . 7500
108 5 CHECKING 10MAY2019 . 1200
109 6 CHECKING 15JUN2020 . 1800
110 7 SAVINGS 30JUL2021 . 15000
111 8 CHECKING 05SEP2018 . 900
112 9 CHECKING 20SEP2019 . 3000
113 9 SAVINGS 20SEP2019 . 12000
114 10 CHECKING 05NOV2020 . 1700
115 3 SAVINGS 20MAR2017 15JUN2019 4000
;
RUN;

/* Generate test transaction data for analysis */
DATA sasdata.generated_transactions;
    /* Parameters for transaction generation */
    ARRAY cust_ids[10] _TEMPORARY_ (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    ARRAY acct_ids[15] _TEMPORARY_ (101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115);
    ARRAY txn_types[4] _TEMPORARY_ ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT');
    ARRAY status_vals[3] _TEMPORARY_ ('ACTIVE', 'PENDING', 'CLOSED');
    
    /* Generate 100 transactions */
    DO i = 1 TO 100;
        /* Randomly select customer, account, and transaction values */
        cust_idx = CEIL(RANUNI(123) * 10);
        acct_idx = CEIL(RANUNI(456) * 15);
        txn_idx = CEIL(RANUNI(789) * 4);
        status_idx = CEIL(RANUNI(012) * 3);
        
        /* Assign values */
        customer_id = cust_ids[cust_idx];
        account_id = acct_ids[acct_idx];
        transaction_date = TODAY() - CEIL(RANUNI(345) * 365);
        transaction_type = txn_types[txn_idx];
        
        /* Generate realistic amounts based on transaction type */
        SELECT (transaction_type);
            WHEN ('DEPOSIT') amount = ROUND(RANUNI(567) * 5000, 0.01);
            WHEN ('WITHDRAWAL') amount = -ROUND(RANUNI(678) * 2000, 0.01);
            WHEN ('TRANSFER') amount = ROUND((RANUNI(789) * 2000) - 1000, 0.01);
            WHEN ('PAYMENT') amount = -ROUND(RANUNI(890) * 1000, 0.01);
            OTHERWISE amount = 0;
        END;
        
        /* Track running balance */
        IF i = 1 THEN balance = 1000 + amount;
        ELSE balance = balance + amount;
        
        status = status_vals[status_idx];
        
        /* Create description based on transaction type */
        SELECT (transaction_type);
            WHEN ('DEPOSIT') description = CATS('Deposit at branch #', CEIL(RANUNI(901) * 50));
            WHEN ('WITHDRAWAL') description = CATS('ATM withdrawal #', CEIL(RANUNI(112) * 100));
            WHEN ('TRANSFER') description = CATS('Transfer with account #', 100 + CEIL(RANUNI(223) * 100));
            WHEN ('PAYMENT') description = CATS('Payment to vendor #', CEIL(RANUNI(334) * 200));
            OTHERWISE description = 'Unknown transaction';
        END;
        
        OUTPUT;
    END;
    
    DROP i cust_idx acct_idx txn_idx status_idx;
RUN;

/* Combine real and generated transaction data */
DATA sasdata.transactions;
    SET sasdata.raw_transactions sasdata.generated_transactions;
    BY customer_id;
RUN;

/* Apply standard transformations */
%standard_transformations(
    input_ds=sasdata.transactions,
    output_ds=sasdata.transactions_transformed
);

/* Macro for account analysis */
%MACRO analyze_accounts(account_type=);
    /* Filter accounts by type */
    PROC SQL;
        CREATE TABLE work.accounts_&account_type AS
        SELECT
            a.account_id,
            a.customer_id,
            a.account_type,
            a.open_date,
            a.close_date,
            a.initial_balance,
            c.first_name,
            c.last_name,
            c.customer_type,
            c.credit_score,
            CASE 
                WHEN a.close_date IS NULL THEN 'Active'
                ELSE 'Closed'
            END AS account_status
        FROM
            sasdata.accounts a
            INNER JOIN sasdata.customers c ON a.customer_id = c.customer_id
        WHERE
            UPCASE(a.account_type) = UPCASE("&account_type")
        ORDER BY
            a.account_id;
    QUIT;
    
    /* Calculate account age */
    DATA work.accounts_&account_type;
        SET work.accounts_&account_type;
        IF close_date = . THEN account_age_days = INTCK('DAY', open_date, &reporting_date);
        ELSE account_age_days = INTCK('DAY', open_date, close_date);
        
        account_age_years = account_age_days / 365.25;
        FORMAT account_age_years 5.2;
    RUN;
    
    /* Summarize account metrics */
    PROC MEANS DATA=work.accounts_&account_type NOPRINT;
        VAR account_age_days credit_score initial_balance;
        OUTPUT OUT=work.account_summary_&account_type
            MEAN(account_age_days)=avg_account_age_days
            MEAN(credit_score)=avg_credit_score
            MEAN(initial_balance)=avg_initial_balance
            MIN(account_age_days)=min_account_age_days
            MIN(credit_score)=min_credit_score
            MIN(initial_balance)=min_initial_balance
            MAX(account_age_days)=max_account_age_days
            MAX(credit_score)=max_credit_score
            MAX(initial_balance)=max_initial_balance;
    RUN;
    
    /* Generate account summary report */
    TITLE "Account Summary for &account_type Accounts";
    PROC PRINT DATA=work.account_summary_&account_type NOOBS;
    RUN;
    TITLE;
    
    /* Transaction analysis by account */
    PROC SQL;
        CREATE TABLE work.account_transactions_&account_type AS
        SELECT
            t.account_id,
            a.customer_id,
            a.account_type,
            a.open_date,
            COUNT(*) AS transaction_count,
            MIN(t.transaction_date) AS first_transaction_date,
            MAX(t.transaction_date) AS last_transaction_date,
            SUM(t.amount) AS total_amount,
            AVG(t.amount) AS avg_amount,
            MIN(t.amount) AS min_amount,
            MAX(t.amount) AS max_amount,
            SUM(CASE WHEN t.amount > 0 THEN 1 ELSE 0 END) AS deposit_count,
            SUM(CASE WHEN t.amount < 0 THEN 1 ELSE 0 END) AS withdrawal_count,
            SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS total_deposits,
            SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END) AS total_withdrawals
        FROM
            sasdata.transactions_transformed t
            INNER JOIN work.accounts_&account_type a ON t.account_id = a.account_id
        GROUP BY
            t.account_id,
            a.customer_id,
            a.account_type,
            a.open_date
        ORDER BY
            t.account_id;
    QUIT;
    
    /* Generate transaction summary report */
    TITLE "Transaction Summary for &account_type Accounts";
    PROC PRINT DATA=work.account_transactions_&account_type;
        VAR account_id customer_id transaction_count total_amount deposit_count total_deposits
            withdrawal_count total_withdrawals;
    RUN;
    TITLE;
    
    /* Calculate transaction metrics by month */
    PROC SQL;
        CREATE TABLE work.monthly_transactions_&account_type AS
        SELECT
            YEAR(t.transaction_date) AS transaction_year,
            MONTH(t.transaction_date) AS transaction_month,
            COUNT(*) AS transaction_count,
            SUM(t.amount) AS total_amount,
            AVG(t.amount) AS avg_amount,
            COUNT(DISTINCT t.account_id) AS active_accounts
        FROM
            sasdata.transactions_transformed t
            INNER JOIN work.accounts_&account_type a ON t.account_id = a.account_id
        GROUP BY
            CALCULATED transaction_year,
            CALCULATED transaction_month
        ORDER BY
            CALCULATED transaction_year,
            CALCULATED transaction_month;
    QUIT;
    
    /* Visualize monthly transaction trends */
    TITLE "Monthly Transaction Trends for &account_type Accounts";
    PROC SGPLOT DATA=work.monthly_transactions_&account_type;
        VBAR transaction_month / RESPONSE=transaction_count GROUP=transaction_year;
        XAXIS LABEL="Month";
        YAXIS LABEL="Transaction Count";
    RUN;
    TITLE;
%MEND analyze_accounts;

/* Run account analysis for each account type */
%analyze_accounts(account_type=CHECKING);
%analyze_accounts(account_type=SAVINGS);

/* Create customer segments based on transaction behavior */
PROC SQL;
    CREATE TABLE sasdata.customer_segments AS
    SELECT
        t.customer_id,
        c.first_name,
        c.last_name,
        c.customer_type,
        c.credit_score,
        COUNT(*) AS transaction_count,
        AVG(ABS(t.amount)) AS avg_transaction_size,
        SUM(CASE WHEN t.amount > 0 THEN 1 ELSE 0 END) AS deposit_count,
        SUM(CASE WHEN t.amount < 0 THEN 1 ELSE 0 END) AS withdrawal_count,
        SUM(CASE WHEN t.large_transaction_flag = 1 THEN 1 ELSE 0 END) AS large_transaction_count,
        MAX(t.balance) AS max_balance,
        MIN(t.balance) AS min_balance,
        
        /* Calculate recency, frequency, monetary metrics */
        INTCK('DAY', MAX(t.transaction_date), &reporting_date) AS days_since_last_transaction,
        COUNT(*) / (INTCK('DAY', MIN(t.transaction_date), MAX(t.transaction_date)) + 1) AS transaction_frequency,
        SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS total_monetary_inflow,
        
        /* Assign segment */
        CASE
            WHEN COUNT(*) > 20 AND AVG(ABS(t.amount)) > 1000 THEN 'High-Value Active'
            WHEN COUNT(*) > 20 THEN 'Active Regular'
            WHEN COUNT(*) > 10 AND AVG(ABS(t.amount)) > 1000 THEN 'High-Value Moderate'
            WHEN COUNT(*) > 10 THEN 'Moderate Regular'
            WHEN COUNT(*) > 5 THEN 'Low Activity'
            ELSE 'Inactive'
        END AS customer_segment
        
    FROM
        sasdata.transactions_transformed t
        INNER JOIN sasdata.customers c ON t.customer_id = c.customer_id
    GROUP BY
        t.customer_id,
        c.first_name,
        c.last_name,
        c.customer_type,
        c.credit_score
    ORDER BY
        t.customer_id;
QUIT;

/* Create summary statistics by customer segment */
PROC MEANS DATA=sasdata.customer_segments NOPRINT;
    CLASS customer_segment;
    VAR transaction_count avg_transaction_size credit_score 
        days_since_last_transaction transaction_frequency total_monetary_inflow;
    OUTPUT OUT=work.segment_summary
        MEAN= STD= MIN= MAX= / AUTONAME;
RUN;

/* Generate customer segment report */
TITLE "Customer Segment Analysis";
PROC PRINT DATA=work.segment_summary;
    BY customer_segment;
    VAR transaction_count_Mean transaction_count_StdDev
        avg_transaction_size_Mean avg_transaction_size_StdDev
        credit_score_Mean
        days_since_last_transaction_Mean
        transaction_frequency_Mean
        total_monetary_inflow_Mean;
RUN;
TITLE;

/* Daily transaction summary */
PROC SQL;
    CREATE TABLE sasdata.daily_summary AS
    SELECT
        transaction_date,
        COUNT(*) AS transaction_count,
        SUM(amount) AS net_amount,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS total_withdrawals,
        COUNT(DISTINCT account_id) AS active_accounts,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM
        sasdata.transactions_transformed
    GROUP BY
        transaction_date
    ORDER BY
        transaction_date;
QUIT;

/* Calculate rolling averages */
DATA sasdata.daily_summary_rolling;
    SET sasdata.daily_summary;
    BY transaction_date;
    
    /* Calculate 7-day rolling metrics */
    ARRAY metrics[5] transaction_count net_amount total_deposits total_withdrawals active_accounts;
    ARRAY rolling_sum[5] _TEMPORARY_;
    ARRAY rolling_avg[5] rolling_transaction_count rolling_net_amount rolling_total_deposits
                        rolling_total_withdrawals rolling_active_accounts;
    
    /* Initialize rolling sums */
    IF _N_ = 1 THEN DO;
        DO i = 1 TO 5;
            rolling_sum[i] = 0;
        END;
    END;
    
    /* Update rolling sums and calculate averages */
    DO i = 1 TO 5;
        rolling_sum[i] = rolling_sum[i] + metrics[i];
        
        IF _N_ <= 7 THEN DO;
            rolling_avg[i] = rolling_sum[i] / _N_;
        END;
        ELSE DO;
            /* Subtract the value from 7 days ago */
            SET sasdata.daily_summary(KEEP=transaction_count net_amount total_deposits 
                                         total_withdrawals active_accounts 
                                     RENAME=(transaction_count=old_count
                                             net_amount=old_net
                                             total_deposits=old_deposits
                                             total_withdrawals=old_withdrawals
                                             active_accounts=old_active))
                                     POINT=(_N_ - 7);
            
            ARRAY old_metrics[5] old_count old_net old_deposits old_withdrawals old_active;
            rolling_sum[i] = rolling_sum[i] - old_metrics[i];
            rolling_avg[i] = rolling_sum[i] / 7;
        END;
    END;
    
    DROP i old_: ;
RUN;

/* Analyze transaction patterns by day of week */
DATA sasdata.day_of_week_analysis;
    SET sasdata.transactions_transformed;
    day_of_week = WEEKDAY(transaction_date);
    
    LABEL day_of_week = 'Day of Week (1=Sunday, 7=Saturday)';
RUN;

PROC MEANS DATA=sasdata.day_of_week_analysis NOPRINT;
    CLASS day_of_week;
    VAR amount positive_amount negative_amount;
    OUTPUT OUT=sasdata.day_of_week_summary
        MEAN(amount)=avg_amount
        SUM(amount)=total_amount
        SUM(positive_amount)=total_deposits
        SUM(negative_amount)=total_withdrawals
        N=transaction_count;
RUN;

/* Export final datasets for reporting */
PROC EXPORT DATA=sasdata.customer_segments
    OUTFILE="/path/to/exports/customer_segments.csv"
    DBMS=CSV REPLACE;
RUN;

PROC EXPORT DATA=sasdata.daily_summary_rolling
    OUTFILE="/path/to/exports/daily_summary.csv"
    DBMS=CSV REPLACE;
RUN;

PROC EXPORT DATA=sasdata.day_of_week_summary
    OUTFILE="/path/to/exports/day_of_week_analysis.csv"
    DBMS=CSV REPLACE;
RUN;

/* Create final transaction report */
TITLE "Transaction Analysis Report";
TITLE2 "Generated on %SYSFUNC(TODAY(), WORDDATE.)";

PROC REPORT DATA=sasdata.daily_summary_rolling;
    COLUMN transaction_date transaction_count total_deposits total_withdrawals
            rolling_transaction_count rolling_total_deposits rolling_total_withdrawals;
    
    DEFINE transaction_date / DISPLAY 'Date' FORMAT=DATE9.;
    DEFINE transaction_count / DISPLAY 'Daily Transactions' FORMAT=COMMA10.;
    DEFINE total_deposits / DISPLAY 'Total Deposits' FORMAT=DOLLAR12.2;
    DEFINE total_withdrawals / DISPLAY 'Total Withdrawals' FORMAT=DOLLAR12.2;
    DEFINE rolling_transaction_count / DISPLAY '7-Day Avg Transactions' FORMAT=COMMA10.1;
    DEFINE rolling_total_deposits / DISPLAY '7-Day Avg Deposits' FORMAT=DOLLAR12.2;
    DEFINE rolling_total_withdrawals / DISPLAY '7-Day Avg Withdrawals' FORMAT=DOLLAR12.2;
    
    /* Only show last 30 days */
    WHERE transaction_date > INTNX('DAY', &reporting_date, -30);
RUN;

/* Clean up work tables */
PROC DATASETS LIBRARY=WORK NOLIST;
    DELETE accounts_: account_summary_: account_transactions_: 
           monthly_transactions_: segment_summary;
QUIT;

/* End of program */ 