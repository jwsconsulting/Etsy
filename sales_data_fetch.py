# The purpose of this is to retrieve Etsy sales data and insert it into a SQL database (10/1/2025)

import requests
import json
import os
from datetime import datetime, timedelta
import pyodbc

# ============================================================================
# CONFIGURATION SECTION - Update these values with your information
# ============================================================================

# Etsy App Credentials (from developer portal)
CLIENT_ID = "your_client_id_here"       # Found in your Etsy app
CLIENT_SECRET = "your_client_secret_here"
REDIRECT_URI = "http://localhost:8000/callback"  # must match what you registered
SCOPES = "transactions_r shops_r"       # Adjust if you need more

SHOP_ID = "your_shop_id_here"

# Token storage (local JSON file)
TOKENS_FILE = "etsy_tokens.json"

# Data Retrieval Settings
DAYS_BACK = 30

# SQL Server Database Settings
SQL_SERVER = "your_server_name"
SQL_DATABASE = "etsy"
SQL_ORDERS_TABLE = "etsy_orders"
SQL_ITEMS_TABLE = "etsy_orders_items"
USE_WINDOWS_AUTH = True

# Export Settings
EXPORT_CSV = False
CSV_ORDERS_FILENAME = "etsy_orders.csv"
CSV_ITEMS_FILENAME = "etsy_items.csv"

# ============================================================================
# END CONFIGURATION SECTION
# ============================================================================


# ---------------------------------------------------------------------------
# OAUTH 2.0 HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def load_tokens():
    """Load access/refresh tokens from file if available."""
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_tokens(tokens):
    """Save tokens to disk."""
    with open(TOKENS_FILE, "w") as f:
        json.dump(tokens, f, indent=2)


def get_authorization_url():
    """Generate the Etsy OAuth2 authorization URL."""
    return (
        f"https://www.etsy.com/oauth/connect"
        f"?response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPES}"
        f"&client_id={CLIENT_ID}"
    )


def exchange_code_for_tokens(auth_code):
    """Exchange authorization code for access and refresh tokens."""
    token_url = "https://openapi.etsy.com/v3/public/oauth/token"
    resp = requests.post(token_url, data={
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "code": auth_code,
    }, auth=(CLIENT_ID, CLIENT_SECRET))
    resp.raise_for_status()
    tokens = resp.json()
    save_tokens(tokens)
    return tokens


def refresh_access_token(refresh_token):
    """Use refresh_token to get a new access_token."""
    token_url = "https://openapi.etsy.com/v3/public/oauth/token"
    resp = requests.post(token_url, data={
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": refresh_token,
    }, auth=(CLIENT_ID, CLIENT_SECRET))
    resp.raise_for_status()
    tokens = resp.json()
    save_tokens(tokens)
    return tokens


def get_valid_access_token():
    """
    Return a valid access token. If expired, refresh it.
    If no tokens exist, guide the user through the OAuth process.
    """
    tokens = load_tokens()

    if not tokens:
        print("No tokens found. First-time setup required.")
        print("1. Visit this URL and authorize the app:")
        print(get_authorization_url())
        auth_code = input("2. Paste the 'code' parameter from the redirected URL here: ")
        tokens = exchange_code_for_tokens(auth_code)

    # Check expiry
    expires_at = tokens.get("expires_at")
    if not expires_at:
        # calculate expiry timestamp from now + expires_in
        expires_in = tokens.get("expires_in", 3600)
        tokens["expires_at"] = datetime.now().timestamp() + expires_in
        save_tokens(tokens)

    if datetime.now().timestamp() >= tokens["expires_at"]:
        print("Access token expired. Refreshing...")
        tokens = refresh_access_token(tokens["refresh_token"])
        tokens["expires_at"] = datetime.now().timestamp() + tokens.get("expires_in", 3600)
        save_tokens(tokens)

    return tokens["access_token"]


class EtsySalesRetriever:
    """
    Retrieves sales data from Etsy API v3.
    
    To use this script:
    1. Create an Etsy app at https://www.etsy.com/developers/your-apps
    2. Get your API key (keystring) and shop_id
    3. Generate an OAuth token with the required scopes
    """
    
    def __init__(self, api_key, shop_id, access_token):
        self.api_key = api_key
        self.shop_id = shop_id
        self.access_token = access_token
        self.base_url = "https://openapi.etsy.com/v3"
        
    def get_headers(self):
        """Return headers for API requests"""
        return {
            "x-api-key": self.api_key,
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def get_receipts(self, limit=100, offset=0, min_created=None, max_created=None):
        """
        Get shop receipts (orders).
        
        Args:
            limit: Number of receipts to retrieve (max 100)
            offset: Pagination offset
            min_created: Unix timestamp for earliest receipt
            max_created: Unix timestamp for latest receipt
        """
        url = f"{self.base_url}/application/shops/{self.shop_id}/receipts"
        
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if min_created:
            params["min_created"] = min_created
        if max_created:
            params["max_created"] = max_created
            
        response = requests.get(url, headers=self.get_headers(), params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
    
    def get_transactions_by_receipt(self, receipt_id):
        """Get all transactions (line items) for a specific receipt"""
        url = f"{self.base_url}/application/shops/{self.shop_id}/receipts/{receipt_id}/transactions"
        
        response = requests.get(url, headers=self.get_headers())
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error getting transactions for receipt {receipt_id}: {response.status_code}")
            return None
    
    def get_all_sales_with_items(self, days_back=30):
        """
        Retrieve all sales data with line items for the specified time period.
        
        Args:
            days_back: Number of days to look back from today
            
        Returns:
            tuple: (receipts_list, transactions_list)
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        min_created = int(start_date.timestamp())
        max_created = int(end_date.timestamp())
        
        all_receipts = []
        all_transactions = []
        offset = 0
        limit = 100
        
        print(f"Fetching sales from {start_date.date()} to {end_date.date()}...")
        
        # Get all receipts
        while True:
            data = self.get_receipts(limit=limit, offset=offset, 
                                    min_created=min_created, max_created=max_created)
            
            if not data or 'results' not in data:
                break
                
            receipts = data['results']
            if not receipts:
                break
                
            all_receipts.extend(receipts)
            print(f"Retrieved {len(all_receipts)} orders so far...")
            
            offset += limit
            
            if len(receipts) < limit:
                break
        
        # Get transactions (line items) for each receipt
        print(f"\nFetching line items for {len(all_receipts)} orders...")
        for i, receipt in enumerate(all_receipts, 1):
            receipt_id = receipt.get('receipt_id')
            if receipt_id:
                transactions_data = self.get_transactions_by_receipt(receipt_id)
                if transactions_data and 'results' in transactions_data:
                    all_transactions.extend(transactions_data['results'])
                    
            if i % 10 == 0:
                print(f"Processed {i}/{len(all_receipts)} orders...")
        
        print(f"Total line items retrieved: {len(all_transactions)}")
        
        return all_receipts, all_transactions
    
    def upsert_to_sql_server(self, receipts, transactions, server, database, 
                            orders_table, items_table, use_windows_auth=True):
        """
        Upsert sales data to SQL Server with two normalized tables.
        
        Args:
            receipts: List of receipt dictionaries (order-level)
            transactions: List of transaction dictionaries (line items)
            server: SQL Server instance name
            database: Database name
            orders_table: Target table for orders
            items_table: Target table for line items
            use_windows_auth: Use Windows Authentication (True) or SQL Auth (False)
        """
        if not receipts:
            print("No receipts to upload")
            return
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
        
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Create orders table if it doesn't exist
            create_orders_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{orders_table}')
            BEGIN
                CREATE TABLE {orders_table} (
                    receipt_id BIGINT PRIMARY KEY,
                    order_date DATETIME,
                    buyer_name NVARCHAR(255),
                    buyer_email NVARCHAR(255),
                    buyer_user_id BIGINT,
                    subtotal DECIMAL(10, 2),
                    total_tax DECIMAL(10, 2),
                    total_shipping DECIMAL(10, 2),
                    total_price DECIMAL(10, 2),
                    discount_amount DECIMAL(10, 2),
                    currency NVARCHAR(10),
                    status NVARCHAR(50),
                    items_count INT,
                    ship_name NVARCHAR(255),
                    ship_address1 NVARCHAR(255),
                    ship_address2 NVARCHAR(255),
                    ship_city NVARCHAR(100),
                    ship_state NVARCHAR(100),
                    ship_zip NVARCHAR(20),
                    ship_country NVARCHAR(100),
                    payment_method NVARCHAR(100),
                    message_from_buyer NVARCHAR(MAX),
                    is_gift BIT,
                    last_updated DATETIME DEFAULT GETDATE()
                )
            END
            """
            cursor.execute(create_orders_table_sql)
            
            # Create items table if it doesn't exist
            create_items_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{items_table}')
            BEGIN
                CREATE TABLE {items_table} (
                    transaction_id BIGINT PRIMARY KEY,
                    receipt_id BIGINT,
                    listing_id BIGINT,
                    product_id BIGINT,
                    title NVARCHAR(500),
                    description NVARCHAR(MAX),
                    quantity INT,
                    price DECIMAL(10, 2),
                    shipping_cost DECIMAL(10, 2),
                    sku NVARCHAR(100),
                    product_data NVARCHAR(MAX),
                    variations NVARCHAR(MAX),
                    is_digital BIT,
                    file_data NVARCHAR(MAX),
                    last_updated DATETIME DEFAULT GETDATE(),
                    FOREIGN KEY (receipt_id) REFERENCES {orders_table}(receipt_id)
                )
            END
            """
            cursor.execute(create_items_table_sql)
            conn.commit()
            
            print(f"\nUpserting {len(receipts)} orders to {orders_table}...")
            
            # Upsert orders
            for receipt in receipts:
                created_date = datetime.fromtimestamp(receipt.get('created_timestamp', 0))
                
                grandtotal = receipt.get('grandtotal', {})
                divisor = grandtotal.get('divisor', 100)
                total_price = grandtotal.get('amount', 0) / divisor
                
                subtotal_data = receipt.get('subtotal', {})
                subtotal = subtotal_data.get('amount', 0) / subtotal_data.get('divisor', 100)
                
                total_tax_data = receipt.get('total_tax_cost', {})
                total_tax = total_tax_data.get('amount', 0) / total_tax_data.get('divisor', 100)
                
                total_shipping_data = receipt.get('total_shipping_cost', {})
                total_shipping = total_shipping_data.get('amount', 0) / total_shipping_data.get('divisor', 100)
                
                discount_data = receipt.get('discount_amt', {})
                discount_amount = discount_data.get('amount', 0) / discount_data.get('divisor', 100)
                
                merge_sql = f"""
                MERGE {orders_table} AS target
                USING (SELECT ? AS receipt_id) AS source
                ON target.receipt_id = source.receipt_id
                WHEN MATCHED THEN
                    UPDATE SET
                        order_date = ?, buyer_name = ?, buyer_email = ?, buyer_user_id = ?,
                        subtotal = ?, total_tax = ?, total_shipping = ?, total_price = ?,
                        discount_amount = ?, currency = ?, status = ?, items_count = ?,
                        ship_name = ?, ship_address1 = ?, ship_address2 = ?, ship_city = ?,
                        ship_state = ?, ship_zip = ?, ship_country = ?, payment_method = ?,
                        message_from_buyer = ?, is_gift = ?, last_updated = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (receipt_id, order_date, buyer_name, buyer_email, buyer_user_id,
                            subtotal, total_tax, total_shipping, total_price, discount_amount,
                            currency, status, items_count, ship_name, ship_address1, ship_address2,
                            ship_city, ship_state, ship_zip, ship_country, payment_method,
                            message_from_buyer, is_gift)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                
                params = (
                    receipt.get('receipt_id', 0),
                    created_date, receipt.get('name', ''), receipt.get('buyer_email', ''),
                    receipt.get('buyer_user_id'), subtotal, total_tax, total_shipping,
                    total_price, discount_amount, grandtotal.get('currency_code', 'USD'),
                    receipt.get('status', ''), len(receipt.get('transactions', [])),
                    receipt.get('name', ''), receipt.get('first_line', ''),
                    receipt.get('second_line', ''), receipt.get('city', ''),
                    receipt.get('state', ''), receipt.get('zip', ''),
                    receipt.get('country_iso', ''), receipt.get('payment_method', ''),
                    receipt.get('message_from_buyer', ''), 1 if receipt.get('is_gift') else 0,
                    # INSERT values
                    receipt.get('receipt_id', 0), created_date, receipt.get('name', ''),
                    receipt.get('buyer_email', ''), receipt.get('buyer_user_id'),
                    subtotal, total_tax, total_shipping, total_price, discount_amount,
                    grandtotal.get('currency_code', 'USD'), receipt.get('status', ''),
                    len(receipt.get('transactions', [])), receipt.get('name', ''),
                    receipt.get('first_line', ''), receipt.get('second_line', ''),
                    receipt.get('city', ''), receipt.get('state', ''), receipt.get('zip', ''),
                    receipt.get('country_iso', ''), receipt.get('payment_method', ''),
                    receipt.get('message_from_buyer', ''), 1 if receipt.get('is_gift') else 0
                )
                
                cursor.execute(merge_sql, params)
            
            conn.commit()
            print(f"Successfully upserted {len(receipts)} orders to {orders_table}")
            
            print(f"\nUpserting {len(transactions)} line items to {items_table}...")
            
            # Upsert line items
            for transaction in transactions:
                price_data = transaction.get('price', {})
                price = price_data.get('amount', 0) / price_data.get('divisor', 100)
                
                shipping_data = transaction.get('shipping_cost', {})
                shipping_cost = shipping_data.get('amount', 0) / shipping_data.get('divisor', 100)
                
                variations = json.dumps(transaction.get('variations', [])) if transaction.get('variations') else None
                product_data = json.dumps(transaction.get('product_data', [])) if transaction.get('product_data') else None
                file_data = json.dumps(transaction.get('file_data', [])) if transaction.get('file_data') else None
                
                merge_sql = f"""
                MERGE {items_table} AS target
                USING (SELECT ? AS transaction_id) AS source
                ON target.transaction_id = source.transaction_id
                WHEN MATCHED THEN
                    UPDATE SET
                        receipt_id = ?, listing_id = ?, product_id = ?, title = ?,
                        description = ?, quantity = ?, price = ?, shipping_cost = ?,
                        sku = ?, product_data = ?, variations = ?, is_digital = ?,
                        file_data = ?, last_updated = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (transaction_id, receipt_id, listing_id, product_id, title, description,
                            quantity, price, shipping_cost, sku, product_data, variations,
                            is_digital, file_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                
                params = (
                    transaction.get('transaction_id', 0),
                    transaction.get('receipt_id', 0), transaction.get('listing_id', 0),
                    transaction.get('product_id'), transaction.get('title', ''),
                    transaction.get('description', ''), transaction.get('quantity', 0),
                    price, shipping_cost, transaction.get('sku', ''),
                    product_data, variations, 1 if transaction.get('is_digital') else 0,
                    file_data,
                    # INSERT values
                    transaction.get('transaction_id', 0), transaction.get('receipt_id', 0),
                    transaction.get('listing_id', 0), transaction.get('product_id'),
                    transaction.get('title', ''), transaction.get('description', ''),
                    transaction.get('quantity', 0), price, shipping_cost,
                    transaction.get('sku', ''), product_data, variations,
                    1 if transaction.get('is_digital') else 0, file_data
                )
                
                cursor.execute(merge_sql, params)
            
            conn.commit()
            print(f"Successfully upserted {len(transactions)} line items to {items_table}")
            
            cursor.close()
            conn.close()
            
        except pyodbc.Error as e:
            print(f"Database error: {e}")
            raise
        except Exception as e:
            print(f"Error: {e}")
            raise
    
    def export_to_csv(self, receipts, transactions, orders_filename, items_filename):
        """Export sales data to CSV files"""
        import csv
        
        if not receipts:
            print("No data to export")
            return
        
        # Export orders
        with open(orders_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Receipt ID', 'Order Date', 'Buyer Name', 'Buyer Email',
                'Total Price', 'Currency', 'Status', 'Items Count'
            ])
            
            for receipt in receipts:
                created_date = datetime.fromtimestamp(receipt.get('created_timestamp', 0))
                grandtotal = receipt.get('grandtotal', {})
                total_price = grandtotal.get('amount', 0) / grandtotal.get('divisor', 100)
                
                writer.writerow([
                    receipt.get('receipt_id', ''),
                    created_date.strftime('%Y-%m-%d %H:%M:%S'),
                    receipt.get('name', ''),
                    receipt.get('buyer_email', ''),
                    total_price,
                    grandtotal.get('currency_code', 'USD'),
                    receipt.get('status', ''),
                    len(receipt.get('transactions', []))
                ])
        
        print(f"Orders exported to {orders_filename}")
        
        # Export line items
        with open(items_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Transaction ID', 'Receipt ID', 'Listing ID', 'Title',
                'Quantity', 'Price', 'SKU', 'Variations'
            ])
            
            for transaction in transactions:
                price_data = transaction.get('price', {})
                price = price_data.get('amount', 0) / price_data.get('divisor', 100)
                variations = json.dumps(transaction.get('variations', []))
                
                writer.writerow([
                    transaction.get('transaction_id', ''),
                    transaction.get('receipt_id', ''),
                    transaction.get('listing_id', ''),
                    transaction.get('title', ''),
                    transaction.get('quantity', 0),
                    price,
                    transaction.get('sku', ''),
                    variations
                ])
        
        print(f"Line items exported to {items_filename}")


# ---------------------------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Step 1: Ensure we have a valid token
    access_token = get_valid_access_token()

    # Step 2: Initialize retriever
    retriever = EtsySalesRetriever(CLIENT_ID, SHOP_ID, access_token)

    # Step 3: Run data retrieval
    receipts_data, transactions_data = retriever.get_all_sales_with_items(days_back=DAYS_BACK)

    print(f"\n{'='*60}")
    print(f"Total orders retrieved: {len(receipts_data)}")
    print(f"Total line items retrieved: {len(transactions_data)}")
    print(f"{'='*60}\n")

    # Step 4: Insert into SQL Server
    retriever.upsert_to_sql_server(
        receipts_data,
        transactions_data,
        server=SQL_SERVER,
        database=SQL_DATABASE,
        orders_table=SQL_ORDERS_TABLE,
        items_table=SQL_ITEMS_TABLE,
        use_windows_auth=USE_WINDOWS_AUTH
    )

    # Step 5: Optional CSV export
    if EXPORT_CSV:
        retriever.export_to_csv(
            receipts_data,
            transactions_data,
            orders_filename=CSV_ORDERS_FILENAME,
            items_filename=CSV_ITEMS_FILENAME
        )
