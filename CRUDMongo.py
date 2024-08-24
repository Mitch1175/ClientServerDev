from pymongo import MongoClient
from bson.objectid import ObjectId

class CRUDMongo(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, user, passwd, host, port, database, collection):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the aac database, the 
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual apporto environment.
        #
        # You must edit the connection variables below to reflect
        # your own instance of MongoDB!
        #
        # Connection Variables
        #
        ## USER = 'aacuser'
        USER = user
        ## PASS = 'SNHU1234'
        PASS = passwd
        ## HOST = 'nv-desktop-services.apporto.com'
        HOST = host
        ## PORT = 30317
        PORT = port
        ## DB = 'aac'
        DB = database
        ## COL = 'animals'
        COL = collection
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

# Return next available record number
    def getNextRecordNum(self):
        # find will always return a cursor even though there should only be one 
        # object here. Get the record with the largest record number so that we can
        # return a number one higher to be used by the create operation to make sure
        # that our records are indexed.
        out = self.collection.find().sort([("rec_num", -1)]).limit(1)
        for dict in out:
            return (dict["rec_num"] + 1)
        

# Complete this create method to implement the C in CRUD.
    def create(self, data):
        if data is not None:
            # Data should always be a list of dictionaries
            # This alows a single method to add one or more dictionaries
            # each representing a single data record.
            for i in data:
                # i should be a dictionary from the list. 
                # this is where we run getNextRecordNum() to find what the next
                # record number should be and replace that prior to inserting into the database.
                index_num = self.getNextRecordNum()

                # Debug statements
                ## print("Testing in Create")
                ## print(index_num)

                # Update record number
                i.update({"rec_num":index_num})

                # Remove id field if it exists
                i.pop("_id", None)

                # Debug statements
                ## print(i)

                ret = self.collection.insert_one(i) 

                # Debug statements
                # print(ret.inserted_id)

		# Verify that it is a valid ObjectID
                if ret.inserted_id.is_valid(ret.inserted_id):
                     # ObjectID is valid
                     continue
                else:
                     return False
        else:
            raise Exception("Nothing to save, because data parameter is empty")
            return False
        return True


# Create method to implement the R in CRUD. 
    def read(self, data):
        # Data should be in the form of a partial dictionary that can be
        # used as the input for the monogo find command. If no data
        # is provided we will return the first record.
        out = []
        if data is not None:
            # The find command is going to return a cursor to handle
            # multiple values. We need to turn this into a list
            # so that we can access all of the results.
            cursor = self.collection.find(data)
            for doc in cursor:
                 out.append(doc)
            ## Debug commands for testing,,,
            ## numRec = len(out)
            ## print("Returning %d records..." % (numRec))
        else:
            out.append(self.collection.find_one())
        return out


# Method used to update a record in the database
    def update(self, query, data):
        # query should be a query object that will select the appropriate
        # records from the database.
        # data should be in the form of the object that defines the new values
        # of the document (this includes the necessary control parameters for
        # the pymongo update_many() call - and the dictionary that contains the
        # new values.
        ret = self.collection.update_many(query,data)

        # Value returned is the number of modified documents
        return ret.modified_count


# Method used to delete a record from the collection
    def delete(self, query):
        # query should be a query object that will select the appropriate
        # records from the database.
        ret = self.collection.delete_many(query)

        # Value returned is the number of documents removed from the collection
        return ret.deleted_count


# Method used to test connection to database
    def con_test(self):
        out = self.collection.find_one()
        return out


def main():
    print("Main Method")
    print("Setting up Database Connection")
    db = CRUDMongo('aacuser', 'SNHU1234', '127.0.0.1', 27017, 'aac', 'animals')

    print("Connection Test")
    print(db.con_test())

    print("Read Test - no param")
    print(db.read(None))

    print("Read Test - breed")
    rec = db.read({ "breed" : "Labrador Retriever Mix" })
    print(len(rec))
    for i in range(5):
        print(rec[i])

    print("Record Number Test")
    print(db.getNextRecordNum())

    print("Testing in create: (main)")
    dummyRec = []
    dummyRec.append(rec[0])
    result = db.create(dummyRec)
    print("Create Result: %s" % result)

    print("Testing Update:")
    retVal =  db.update({ "animal_id": "B721406" },{ "$set": { "animal_id": "A721406" }})
    print("%d records updated." % retVal)

    print("Testing removal of last document added...")
    lastVal = db.getNextRecordNum() - 1
    retVal = db.delete( { "rec_num" : lastVal } )
    print("%d records deleted from the collection." % retVal)


if __name__ == "__main__":
    main()
