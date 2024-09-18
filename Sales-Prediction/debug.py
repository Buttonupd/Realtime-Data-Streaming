import pandas as pd
from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

class ExtractTransform:

    def __init__(self) -> None:
        pass

    @staticmethod
    def tables_To_Df():
        pd.set_option('display.max_columns', None)

        # Define your database connection string properly
        connection_string = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{databasename}?driver=ODBC+Driver+17+for+SQL+Server')

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
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Model Building
        model = Pipeline(steps=[('preprocessor', preprocessor),
                                ('regressor', LinearRegression())])
        model.fit(X_train, y_train)

        # Predictions
        y_pred = model.predict(X_test)

        # Evaluation
        mse = mean_squared_error(y_test, y_pred)
        print("Mean Squared Error:", mse)

server = "DKARIUKI-PC\BASESSQL_SERVER"
databasename = "UNDT_SACCO_Staging"
username = "sa"
password = "Saf3rthanc0v1d19"

ExtractTransform.tables_To_Df()

