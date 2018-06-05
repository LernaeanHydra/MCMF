package data

import "fmt"

type ResourceI interface {
	PrintResource()
}

type Task struct {
	Index int
	Id string `bson:"_id"`
	UID string `bson:"uid"`
	Cpu int `bson:"cpu"`
	Mem int `bson:"memory"`
	DiskSize int `bson:"disk_size"`
	MatchTag []string `bson:"match_tag"`
	ExcludeTag []string `bson:"exclude_tag"`
	ExclusiveTag []string `bson:"exclusive_tag"`
	IsKeyInstance bool `bson:"is_key_instance"`
	AppGroup string `bson:"app_group"`
	HostId string `bson:"host_id"`

	//TaskRejectedList []*Task
	HeadNode *TemplateNode  // only can be TaskNode or TemplateNode
	Tasknode *TaskNode
}

func (resource *Task)PrintResource()  {
	fmt.Println(resource.UID)
}

func (task *Task)UpdateHeadNode(node *TemplateNode){
	task.HeadNode = node
}

func (task *Task)GetHeadNode() NodeI{
	return task.HeadNode
}

func (task *Task)GetCapacity() []int{
	var keyInstanceNum int
	if task.IsKeyInstance {
		keyInstanceNum = 1
	}else {
		keyInstanceNum = 0
	}
	capacity := []int{task.Cpu, task.Mem, task.DiskSize, keyInstanceNum}
	return capacity
}

//func (task *Task)AddTaskToRejectedList(t *Task) {
//	task.TaskRejectedList = append(task.TaskRejectedList, t)
//}

type Application struct {
	Name string
}

func (resource *Application)PrintResource()  {
	fmt.Println(resource.Name)
}

type Machine struct {
	Index int  // index of ClusterMachineList
	Id string `bson:"_id"`
	UID string `bson:"uid"`
	Cpu int `bson:"cpu"`
	Mem int `bson:"memory"`
	DiskSize int `bson:"disk_size"`
	MaxKeyInstance int `bson:"max_key_instances"`
	MatchTag []string `bson:"match_tag"`
	ExcludeTag []string `bson:"exclude_tag"`

	TaskAcceptedList [][][]*Task
	Node *MachineNode
}

func (resource *Machine)PrintResource()  {
	fmt.Println(resource.UID)
}

func (machine *Machine)UpdateNode(node *MachineNode){
	machine.Node = node
}

func (machine *Machine)GetNode() *MachineNode{
	return machine.Node
}

type NodeI interface {
	PrintSelf()  // used for printing path
	SetID(id int)
	GetID() int
	AddLeftInArc(arc *Arc)
	AddLeftOutArc(arc *Arc)
	AddRightInArc(arc *Arc)
	AddRightOutArc(arc *Arc)
	GetLeftInArcs() []*Arc
	GetLeftOutArcs() []*Arc
	GetRightInArcs() []*Arc
	GetRightOutArcs() []*Arc
}

type MutexableNodeI interface {
	GetSourceTask() []*Task
	GetNotMutexTask() []*Task

}

type Node struct {
	id int  // used for printing path, it's index of NodeList
	leftInArcs []*Arc
	leftOutArcs []*Arc
	rightInArcs []*Arc
	rightOutArcs []*Arc
}

func (node *Node)GetID() int {
	return node.id
}

func (node *Node)SetID(id int)  {
	node.id = id
}

func (node *Node)AddLeftInArc(arc *Arc)  {
	node.leftInArcs = append(node.leftInArcs, arc)
}

func (node *Node)AddLeftOutArc(arc *Arc)  {
	node.leftOutArcs = append(node.leftOutArcs, arc)
}

func (node *Node)AddRightInArc(arc *Arc)  {
	node.rightInArcs = append(node.rightInArcs, arc)
}

func (node *Node)AddRightOutArc(arc *Arc)  {
	node.rightOutArcs = append(node.rightOutArcs, arc)
}

func (node *Node)GetLeftInArcs() []*Arc {
	return node.leftInArcs
}

func (node *Node)GetLeftOutArcs() []*Arc {
	return node.leftOutArcs
}

func (node *Node)GetRightInArcs() []*Arc {
	return node.rightInArcs
}

func (node *Node)GetRightOutArcs() []*Arc {
	return node.rightOutArcs
}

type ApplicationNode struct {
	Node
	Applicaiton *Application
}

func (n *ApplicationNode) PrintSelf()  {
	fmt.Println(n.id)
}

type TaskNode struct {
	Node
	Task *Task
}

func (n *TaskNode) PrintSelf()  {
	fmt.Println(n.id)
}

type MachineNode struct {
	Node
	Machine *Machine
	ScheduledTasks map[string][]int  //exclusive-tag ——> ArcList[] index
}

func (n *MachineNode) PrintSelf()  {
	fmt.Println(n.id)
}

type MutexNode struct {
	Node
	MutexTaskList []*Task
}


func (n *MutexNode) PrintSelf()  {
	fmt.Println(n.id)
}

type CaseNode struct {
	Node
	SourceTasks []*Task
	TaskNotMutex []*Task  // all tasks that shoud mutex but have not been mutexed with this task
}

func (n *CaseNode) PrintSelf()  {
	fmt.Println(n.id)
}

func (n *CaseNode) GetSourceTask() []*Task {
	return n.SourceTasks
}

func (n *CaseNode) GetNotMutexTask() []*Task {
	return n.TaskNotMutex
}


type MetaNode struct {
	Node
	IsEnd bool // false -> start node
}

func (n *MetaNode) PrintSelf()  {
	fmt.Println(n.id)
}

type TemplateNode struct {
	Node
	SourceTasks []*Task
	TaskNotMutex []*Task
	CurrentHeadNode *CaseNode
}

func (n *TemplateNode) PrintSelf()  {
	fmt.Println(n.id)
}

func (n *TemplateNode) GetSourceTask() []*Task {
	return n.SourceTasks
}

func (n *TemplateNode) GetNotMutexTask() []*Task {
	return n.TaskNotMutex
}

type Arc struct {
	ID int
	Capacity []int // size = 4 -> (cup, mem, disk, keyApplicaitonUpperSize)
	Cost int
	SrcNode NodeI
	DstNode NodeI
}

func (arc *Arc) SubCapacity(flow []int){
	for i, _ := range arc.Capacity{
		arc.Capacity[i] -= flow[i]
	}
}

func (arc *Arc) AddCapacity(flow []int){
	for i, _ := range arc.Capacity{
		arc.Capacity[i] += flow[i]
	}
}