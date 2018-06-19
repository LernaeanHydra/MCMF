package data

import "math/rand"

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

var TaskToMachineCost [][]int  // task index ,machine index ——> cost.  dropped
var TaskMinCostArcIdList [][]*Arc // task index , 0\1\2\3\4 ——> *Arc

var RandomArc *rand.Rand  // used for get preemption arc list



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

	// init TaskToMachineCost should be done after DB operation
	randSeed := rand.NewSource(50)
	RandomArc = rand.New(randSeed)
}

/**
	add l2 to l1
 */
func ListAdd(l1 []int, l2 []int){
	for i, _ := range l1{
		l1[i] += l2[i]
	}
}

/**
	sub l1 by l2
 */
func ListSub(l1 []int, l2 []int){
	for i, _ := range l1{
		l1[i] -= l2[i]
	}
}


func GetMiddle(numbers []*Arc, low int, high int) int {
	tmp := numbers[low]
	for low < high {
		for low<high && numbers[high].Cost >= tmp.Cost {
			high--
		}
		numbers[low] = numbers[high]
		for low<high && numbers[low].Cost <= tmp.Cost {
			low++
		}
		numbers[high] = numbers[low]
	}
	numbers[low] = tmp
	return low
}

/**
	quick sort
 */
func QuickSort(numbers []*Arc, low int, high int)  {
	if low < high {
		middle := GetMiddle(numbers, low, high)
		QuickSort(numbers, low, middle-1)
		QuickSort(numbers, middle+1, high)
	}
}



