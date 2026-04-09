import boto3
import csv
import json
from decimal import Decimal
from datetime import datetime

# AWS Clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# CONFIG
TABLE_NAME = 'salesdb'
DATA_BUCKET = 'salesanalysis-bucket-abc'
CSV_KEY = 'data/sales_data.csv'   # ✅ FIX THIS PATH AS PER YOUR S3
REPORT_BUCKET = 'salesanalysis-bucket-abc'

table = dynamodb.Table(TABLE_NAME)


# =====================================================
# Convert Decimal to JSON
# =====================================================
def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


# =====================================================
# Check if item exists in DynamoDB
# =====================================================
def item_exists(id):
    response = table.get_item(Key={"id": id})
    return 'Item' in response


# =====================================================
# Analyze Data
# =====================================================
def analyze_data(items):
    total_sales = 0
    total_orders = 0
    product_map = {}

    for item in items:
        amount = float(item['amount'])
        product = item['product']

        total_sales += amount
        total_orders += 1

        if product in product_map:
            product_map[product] += amount
        else:
            product_map[product] = amount

    if total_orders == 0:
        return None

    top_product = max(product_map.items(), key=lambda x: x[1])

    return {
        "Total Sales": total_sales,
        "Total Orders": total_orders,
        "Top Product": {
            "name": top_product[0],
            "sales": top_product[1]
        },
        "Generated At": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    }


# =====================================================
# MAIN HANDLER
# =====================================================
def lambda_handler(event, context):

    print("⏰ Scheduled Lambda Triggered")

    new_records = 0   # ✅ FIXED (moved outside try)

    # =====================================================
    # 1. READ CSV FROM S3
    # =====================================================
    try:
        print(f"📂 Reading file from: {DATA_BUCKET}/{CSV_KEY}")

        response = s3.get_object(Bucket=DATA_BUCKET, Key=CSV_KEY)
        content = response['Body'].read().decode('utf-8').splitlines()
        csv_reader = csv.DictReader(content)

        # =====================================================
        # 2. INSERT ONLY NEW DATA INTO DYNAMODB
        # =====================================================
        for row in csv_reader:
            id_value = row['id']

            if not item_exists(id_value):   # avoid duplicate
                table.put_item(
                    Item={
                        "id": id_value,
                        "product": row['product'],
                        "amount": Decimal(row['amount'])
                    }
                )
                new_records += 1

        print(f"✅ New records inserted: {new_records}")

    except s3.exceptions.NoSuchKey:
        print("⚠️ CSV file not found in S3. Skipping insert step.")

    except Exception as e:
        print("❌ Error reading CSV:", str(e))

    # =====================================================
    # 3. FETCH ALL DATA FROM DYNAMODB (FULL SCAN)
    # =====================================================
    items = []
    response = table.scan()
    items.extend(response.get('Items', []))

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    print(f"📊 Total records in DB: {len(items)}")

    # =====================================================
    # 4. ANALYZE DATA
    # =====================================================
    report = analyze_data(items)

    if not report:
        return {
            "statusCode": 200,
            "body": "No data available"
        }

    # =====================================================
    # 5. SAVE REPORT TO S3
    # =====================================================
    file_name = f"reports/report_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"

    try:
        s3.put_object(
            Bucket=REPORT_BUCKET,
            Key=file_name,
            Body=json.dumps(report, default=convert_decimal),
            ContentType='application/json'
        )

        print(f"📁 Report saved successfully: {file_name}")

    except Exception as e:
        print("❌ Error saving report:", str(e))
        raise e

    # =====================================================
    # RESPONSE
    # =====================================================
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Scheduled job completed",
            "new_records_added": new_records,
            "report_file": file_name,
            "report": report
        }, default=convert_decimal)
    }