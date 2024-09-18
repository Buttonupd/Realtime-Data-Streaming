import pandas as pd
import warnings
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from systems_connector import SystemConnectors
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import make_column_selector as selector

# Declines by agents and complaints and amount 
#customer, origin description, memberno, customer, disposition, subdisposition, status, businessline.

class ExtractTransform:

    def __init__(self) -> None:
        pass
#JUB-216138-Q3M6
    @staticmethod
    def tables_To_Df():
        pd.set_option('display.max_columns', None)

        connection_string = SystemConnectors.WarehouseConnect(
            server,
            databasename,
            username,
            password
        )
        query = '''
            SELECT *
            FROM [UNDT_SACCO_Staging].[dbo].Transformed_Vendor_LedgerEntry 
            WHERE [Closed_by_Amount] > 0
        '''

        data = pd.read_sql_query(sql=query, con=connection_string)
        
        # Inspect the loaded data
        print("Data loaded from SQL:")
        print(data.head())
        print("Columns in DataFrame:")
        print(data.columns)

        # Check if 'Closed_by_Amount' column exists
        if 'Closed_by_Amount' not in data.columns:
            raise KeyError("'Closed_by_Amount' column is missing from the DataFrame")

        date_columns = ['Posting_Date', 'Due_Date', 'Closed_at_Date', 'Document_Date']
        # Ensure these columns exist before applying datetime conversion
        missing_date_columns = [col for col in date_columns if col not in data.columns]
        if missing_date_columns:
            raise KeyError(f"Missing date columns: {missing_date_columns}")

        data[date_columns] = data[date_columns].apply(pd.to_datetime)
        
        # EDA
        ExtractTransform.perform_eda(data)

        # Ensure all features are numeric
        numeric_features = ['Entry_No_']  # Specify only the numeric columns you want to scale
        print("Numeric features:")
        print(numeric_features)
        
        numeric_transformer = Pipeline(steps=[
            ('scaler', StandardScaler())])

        preprocessor = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, numeric_features)])

        # Split data into features and target
        X = data.drop('Closed_by_Amount', axis=1)
        y = data['Closed_by_Amount']

        # Print X and y to confirm
        print("Features (X):")
        print(X.head())
        print("Target (y):")
        print(y.head())

        # Split into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, train_size=0.7, random_state=42)

        # Model Building
        model = Pipeline(steps=[('preprocessor', preprocessor),
                                ('regressor', LinearRegression())])
        model.fit(X_train, y_train)

        # Predictions
        y_pred = model.predict(X_test)

        # Feature engineering: add predicted values
        X_test['Predicted_Closed_by_Amount'] = y_pred

        # Combine X_test with y_test for a complete DataFrame
        result_df = X_test.copy()
        result_df['Actual_Closed_by_Amount'] = y_test.values

        # Evaluation
        mse = mean_squared_error(y_test, y_pred)
        print("Mean Squared Error:", mse)

        # Calculate Mean Absolute Error (MAE)
        mae = mean_absolute_error(y_test, y_pred)
        print("Mean Absolute Error:", mae)

        # Visual comparison between actual and predicted values
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=result_df, x='Actual_Closed_by_Amount', y='Predicted_Closed_by_Amount', color='blue', label='Actual')
        sns.scatterplot(data=result_df, x='Actual_Closed_by_Amount', y='Predicted_Closed_by_Amount', color='red', label='Predicted')
        plt.xlabel('Actual Closed by Amount')
        plt.ylabel('Predicted Closed by Amount')
        plt.title('Actual vs Predicted')
        plt.legend()
        plt.show()

        # Write back to SQL
        ExtractTransform.write_to_sql(result_df, 'Predicted_Transformed_Vendor_LedgerEntry', connection_string)

    @staticmethod
    def perform_eda(df):
        # Perform EDA using seaborn
        plt.figure(figsize=(10, 6))
        sns.heatmap(df.corr(), annot=True, cmap='coolwarm')
        plt.title('Correlation Matrix')
        plt.show()

        plt.figure(figsize=(10, 6))
        sns.histplot(df['Closed_by_Amount'], bins=30, kde=True)
        plt.title('Distribution of Closed by Amount')
        plt.show()

    @staticmethod
    def transform_columns(df):
        df.columns = [col.replace(' ', '_') for col in df.columns]
        return df

    @staticmethod
    def write_to_sql(df, table_name, connection_string):
        engine = connection_string
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    @staticmethod
    def read_from_sql(table_name, connection_string):
        query = f'SELECT * FROM {table_name}'
        data = pd.read_sql_query(sql=query, con=connection_string)
        return data

# Configuration
server = "DKARIUKI-PC\\BASESSQL_SERVER"
databasename = "UNDT_SACCO_Staging"
username = "sa"
password = "Saf3rthanc0v1d19"

# Instantiate the class
et = ExtractTransform()

# Fetch, process, and analyze data
et.tables_To_Df()
