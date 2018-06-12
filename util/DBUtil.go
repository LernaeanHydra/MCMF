package util

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	data "MCMF/data"
)

// when implementing this interface, you should check it is that struct or it's pointer implemented this interface
type DBReader interface {
	GetTasks()
	GetMachines()
}

type MongoReader struct {
	Url string
	Database string
	Session *mgo.Session

}


/**
DBReaderFactory creates all type of DBReaders
 */
type DBReaderFactory struct {
	Url string
	Database string
}

func (db *DBReaderFactory)GetUrl() string{
	return db.Url
}

func (db *DBReaderFactory)GetDatabase() string{
	return db.Database
}

func (db *DBReaderFactory)GetDBReader(dbType string) DBReader {  // GO LANGUAGE TIPS: the return type is the pointer to this type, not type self
	if dbType == "mongo"{
		ss, err := mgo.Dial(db.GetUrl())
		if err != nil {
			panic("Failed to connect MongoDB")
		}
		return &MongoReader{
			Url:db.GetUrl(),
			Database:db.GetDatabase(),
			Session: ss}
	}else {
		panic("This verison don't support other Database except mongo")  // todo extend this
	}

}


/**
Following part is how mongoReader implements DBReader interface
 */

func (reader *MongoReader)GetUrl() string{
	return reader.Url
}
func (reader *MongoReader)GetDatabase() string{
	return reader.Database
}
func (reader *MongoReader)GetSession() *mgo.Session{
	return reader.Session
}

 // 1、filter all machines, if the machine misses the key message, drop it
 // 2、Sort all machines by its weighted capacity Desc (for now we only consider cpu)
 // 3、return sorted valid machines list
func (reader *MongoReader)GetMachines() {
	collection := reader.Session.DB(reader.Database).C(data.NODECOLLECTION)
	if err := collection.Find(bson.M{}).Sort("uid").All(&data.ClusterMachineList); err != nil{
		panic("Error appears when getting machineList")
	}
	data.MachineSum = len(data.ClusterMachineList)
	// fmt.Println("getMachines method end, and ClusterMachineList's size is", len(data.ClusterMachineList))
	for i, _ := range data.ClusterMachineList {
		data.ClusterMachineList[i].Index = i
	}

}
 // 1、filter all tasks, if the task misses the key message OR the hostId is not empty, drop it
 // 2、get 100 tasks each iteration
 // 3、get []*Task currentTaskList
 // 4、get []*Application currentApplicationList
 // 5、get [][]*Task applicaitonAndTask indicate all tasks the applicaiton has, and it ordered by currentApplicaitonList order
func (reader *MongoReader)GetTasks() {
	collection := reader.Session.DB(reader.Database).C(data.INSTANCECOLLECTION)
	if err := collection.Find(bson.M{}).Limit(500).Sort("uid").All(&data.CurrentTaskList); err != nil{
		panic("Error appears when getting taskList")
	}
	data.TaskSum = len(data.CurrentTaskList)
	for i, _ := range data.CurrentTaskList {
		data.CurrentTaskList[i].Index = i
	}

}


