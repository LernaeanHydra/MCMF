package data

// In GO, all gloabal variables, constant variables, method, struct, interface, member variables should has its first letter capitalized
var ClusterMachineList []Machine
var CurrentTaskList []Task
// var CurrentAppList []Application
var CurrentAppMap map[string]int
var ApplicationAndTask [][][]*Task

var MachineNodes []*MachineNode
var ApplicationNodes []*ApplicationNode
var TaskNodes []*TaskNode
var NodeList []NodeI
var ArcList []*Arc

var StartNode *MetaNode
var EndNode *MetaNode

// var TaskAcceptedList [][]*TaskNode // according to machine node
// var TaskRejectedList [][]*TaskNode // according to task node
// var KeyTaskNodeList []*TaskNode
// var HeadNodeOfTask []*NodeI  // it can be all type

var TaskSum int
var MachineSum int

var NodeCounter int  // it gives ID to Node
var ArcCounter int

var TaskToMachineCost [][]int



func init()  {
	ClusterMachineList = make([]Machine, 0)
	CurrentTaskList = make([]Task, 0)
	//CurrentAppList = make([]Application, 0)
	CurrentAppMap = make(map[string]int)
	ApplicationAndTask = make([][][]*Task, 0)

	MachineNodes = make([]*MachineNode, 0)
	ApplicationNodes = make([]*ApplicationNode, 0)
	TaskNodes = make([]*TaskNode, 0)

	NodeCounter = 0
	ArcCounter = 0
	NodeList = make([]NodeI, 0)

	TaskSum = 0
	MachineSum = 0
	TaskToMachineCost = make([][]int, 0)
}


