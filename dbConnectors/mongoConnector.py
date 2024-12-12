from pymongo import MongoClient

def get_mongo_collection(connection_string, database_name, collection_name):
    """
    Establishes a connection to MongoDB Atlas and returns the collection object.
    
    :param connection_string: MongoDB connection string
    :param database_name: Name of the database
    :param collection_name: Name of the collection
    :return: MongoDB collection object
    """
    client = MongoClient(connection_string)
    db = client[database_name]
    return db[collection_name]
