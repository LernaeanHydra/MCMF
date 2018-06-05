package flowMap

import (
	data "MCMF/data"
	"reflect"
)

type MapBuilderI interface {
	BuildNodesAndArcs()
	BuildArc(src *data.NodeI, dst *data.NodeI) *data.Arc
	BuildNode(nodeType string, resource []*data.ResourceI, isEnd bool) *data.NodeI
}


type MapBuilder struct {
	// this struct only has methods
}

/**
	the capacity will be calculated when this src was built
 */
func (builder *MapBuilder)BuildArc(src data.NodeI, dst data.NodeI) (*data.Arc, *data.Arc) {
	arc := &data.Arc{}
	capacity := make([]int, 1) // todo this version only consider cpu resource
	if node, ok := dst.(*data.ApplicationNode); ok{ // if dst node is applicationNode, its capacity should be the sum of its tasks resources
		index := data.CurrentAppMap[node.Applicaiton.Name]
		for _, tasks := range data.ApplicationAndTask[index]{
			for _, task := range tasks{
				capacity[0] += task.Cpu
			}
		}
	}else if node, ok := dst.(*data.TaskNode); ok { // if dst node is taskNode, its capacity should be the resource of that task resource
		capacity[0] += node.Task.Cpu

	}else if _, ok := dst.(*data.MutexNode); ok {
		if node, ok := src.(*data.TaskNode); ok {  // src: Task, dst: Mutex
			capacity[0] += node.Task.Cpu
		}else if node, ok := src.(*data.CaseNode); ok { // src: Case, dst: Mutex
			capacity[0] += node.SourceTasks[0].Cpu
		}else if node, ok := src.(*data.TemplateNode); ok { // src: Case, dst: Mutex
			capacity[0] += node.SourceTasks[0].Cpu
		}
	}else if node, ok := dst.(*data.CaseNode); ok {  // when the dst node is CaseNode, the capacity should be the resource of its sourceTaskNodes[0]'s resource
		capacity[0] += node.SourceTasks[0].Cpu
	}else if  _, ok := dst.(*data.MachineNode); ok{
		if node, ok := src.(*data.TaskNode); ok {  // src: Task, dst: Mutex
			capacity[0] += node.Task.Cpu
		}else if node, ok := src.(*data.CaseNode); ok { // src: Case, dst: Mutex
			capacity[0] += node.SourceTasks[0].Cpu
		}else if node, ok := src.(*data.TemplateNode); ok { // src: Case, dst: Mutex
			capacity[0] += node.SourceTasks[0].Cpu
		}
	}else if _, ok := dst.(*data.MetaNode); ok {
		if node, ok := src.(*data.MachineNode); ok {
			capacity[0] += node.Machine.Cpu
		}else {
			panic("In this Version, only MachineNode can connect to EndNode")
		}
	}else if node, ok := dst.(*data.TemplateNode); ok {
		capacity[0] += node.SourceTasks[0].Cpu
	}else {
		panic("Illegal map struct: destination Node is " + reflect.TypeOf(dst).Name())
	}
	arc.Capacity = capacity
	arc.Cost = 0
	arc.SrcNode = src
	arc.DstNode = dst
	arc.ID = data.ArcCounter
	data.ArcList = append(data.ArcList, arc)
	data.ArcCounter++


	reverseArc := &data.Arc{}
	reverseArc.DstNode = src
	reverseArc.SrcNode = dst
	reverseArc.Cost = 0
	capacity2 := make([]int, 1)
	capacity2[0] = 0
	reverseArc.Capacity = capacity2
	reverseArc.ID = data.ArcCounter
	data.ArcList = append(data.ArcList, reverseArc)
	data.ArcCounter++
	return arc, reverseArc
}

/**
	this method can't return
 */
func (builder *MapBuilder)BuildArcWithMutex(src data.MutexableNodeI, dst data.NodeI) (*data.Arc, *data.Arc) {
	arc := &data.Arc{}
	capacity := make([]int, 1) // todo this version only consider cpu resource
	if node, ok := src.(*data.CaseNode); ok { // src: Case, dst: Mutex or Machine
		capacity[0] += node.SourceTasks[0].Cpu
		arc.SrcNode = src.(*data.CaseNode)
	}else{ // src: 	Template, dst: Mutex or Machine
		tmpNode := src.(*data.TemplateNode)
		capacity[0] += tmpNode.SourceTasks[0].Cpu
		arc.SrcNode = tmpNode
	}
	arc.Capacity = capacity
	arc.Cost = 0
	arc.DstNode = dst
	arc.ID = data.ArcCounter
	data.ArcList = append(data.ArcList, arc)
	data.ArcCounter++

	reverseArc := &data.Arc{}
	capacity2 := make([]int, 1)
	capacity2[0] = 0
	reverseArc.Capacity = capacity2
	if _, ok := src.(*data.CaseNode); ok {
		reverseArc.DstNode = src.(*data.CaseNode)
	}else{
		reverseArc.DstNode = src.(*data.TemplateNode)
	}

	reverseArc.SrcNode = dst
	reverseArc.Cost = 0
	reverseArc.ID = data.ArcCounter
	data.ArcList = append(data.ArcList, reverseArc)
	data.ArcCounter++
	return arc, reverseArc
}



func (builder *MapBuilder)BuildApplicationNode(resources *data.Application) *data.ApplicationNode {
	var node *data.ApplicationNode
	node = &data.ApplicationNode{Applicaiton: resources}
	node.SetID(data.NodeCounter)
	data.NodeList = append(data.NodeList, node)
	data.NodeCounter++
	return node
}


func (builder *MapBuilder)BuildTaskNode(resources *data.Task) *data.TaskNode {
	var node *data.TaskNode
	node = &data.TaskNode{Task: resources}
	node.SetID(data.NodeCounter)
	data.NodeList = append(data.NodeList, node)
	data.NodeCounter++
	return node
}

func (builder *MapBuilder)BuildMachineNode(resources *data.Machine) *data.MachineNode {
	var node *data.MachineNode
	node = &data.MachineNode{Machine: resources}
	node.SetID(data.NodeCounter)
	data.NodeList = append(data.NodeList, node) // its index is its id
	data.NodeCounter++
	return node
}

func (builder *MapBuilder)BuildMutexNode(resources []*data.Task) *data.MutexNode {
	var node *data.MutexNode
	node = &data.MutexNode{MutexTaskList: resources}
	node.SetID(data.NodeCounter)
	data.NodeList = append(data.NodeList, node)
	data.NodeCounter++
	return node
}

func (builder *MapBuilder)BuildCaseNode(resources []*data.Task) *data.CaseNode {
	var node *data.CaseNode
	node = &data.CaseNode{SourceTasks: resources}
	node.SetID(data.NodeCounter)
	data.NodeList = append(data.NodeList, node)
	data.NodeCounter++
	return node
}
func (builder *MapBuilder)BuildTemplateNode(resources []*data.Task) *data.TemplateNode {
	var node *data.TemplateNode
	node = &data.TemplateNode{SourceTasks: resources}
	node.SetID(data.NodeCounter)
	data.NodeList = append(data.NodeList, node)
	data.NodeCounter++
	return node
}

func (builder *MapBuilder)BuildMetaNode(isEnd bool) *data.MetaNode {
	var node *data.MetaNode
	node = &data.MetaNode{IsEnd: isEnd}
	node.SetID(data.NodeCounter)
	data.NodeList = append(data.NodeList, node)
	data.NodeCounter++
	return node
}

/*
 1、create ApplicationNode and its all taskNodes by ApplicationAndTask, and connect ApplicationNode to all its taskNodes -> ApplicationNodes and TaskNodes
 2、create start MetaNode and connect it to all ApplicationNode
 3、create all MachineNodes and connect them to the end MetaNode
 */
func (builder *MapBuilder)BuildNodes() {
	// create ApplicationNode and its all taskNodes
	data.StartNode = builder.BuildMetaNode(false)
	data.EndNode = builder.BuildMetaNode(true)

	// build startNode, applicationNodes and taskNodes. build all arcs among them
	for index, _ := range data.ApplicationAndTask {
		// we first connect applicationNode and taskNode, then connect startNode to applcationNode.
		application := &data.Application{Name: data.ApplicationAndTask[index][0][0].AppGroup}
		applicationNode := builder.BuildApplicationNode(application)
		data.ApplicationNodes = append(data.ApplicationNodes, applicationNode)  // add this applicationNode to global List, its order is the same as ApplicationAndTask

		for index2, _ := range data.ApplicationAndTask[index] {
			for index3, _:= range data.ApplicationAndTask[index][index2] {
				taskNode := builder.BuildTaskNode(data.ApplicationAndTask[index][index2][index3])  // build taskNode for it
				data.TaskNodes = append(data.TaskNodes, taskNode) // add it to TaskNodes
				arc, reverseArc := builder.BuildArc(applicationNode, taskNode)  // build arc from applicationNode to taskNode
				builder.AddArcTONodes(arc, false)
				builder.AddArcTONodes(reverseArc, true)
				data.ApplicationAndTask[index][index2][index3].Tasknode = taskNode
			}

		}

		// connect startNode to this applicationNode
		arc, reverseArc := builder.BuildArc(data.StartNode, applicationNode)
		builder.AddArcTONodes(arc, false)
		builder.AddArcTONodes(reverseArc, true)

	}

	// build machineNode, endNdoe. build all arcs between them
	for index, _ := range data.ClusterMachineList {
		machineNode := builder.BuildMachineNode(&data.ClusterMachineList[index])
		data.MachineNodes = append(data.MachineNodes, machineNode)
		data.ClusterMachineList[index].Node = machineNode
		arc, reverseArc := builder.BuildArc(machineNode, data.EndNode)
		builder.AddArcTONodes(arc, false)
		builder.AddArcTONodes(reverseArc, true)

	}
}


func (builder *MapBuilder)AddArcTONodes(arc *data.Arc, isReversse bool) {
	if isReversse {
		arc.SrcNode.AddLeftOutArc(arc)
		arc.DstNode.AddRightInArc(arc)
	}else {
		arc.SrcNode.AddRightOutArc(arc)
		arc.DstNode.AddLeftInArc(arc)
	}

}


/**
	1. build taskNode and applicationNode; build machineNode
	2. mutex all same tasks.——> build templateNode
	3. mutex all exclusiveTag task.——> build caseNode
	4. connect all caseNode to machineNode
	tips: the caseNode should hold a list of the excluding-tasks that hasn't been mutexed
 */
func (build *MapBuilder) BuildMap() {
	build.BuildNodes()
	build.BuildTemplate()
	for index, _ := range data.ClusterMachineList {
		//build.BuildConstraints(data.ClusterMachineList[index].TaskAcceptedList)
		build.ConnectMap(data.ClusterMachineList[index].TaskAcceptedList, data.ClusterMachineList[index].Node)
	}
}

/**
	connet all templateNode to machineNode, update TaskToMachineCost global var
 */
func (build *MapBuilder) ConnectMap(acceptedTasks [][][]*data.Task, machineNode *data.MachineNode) {
	for index, _:= range acceptedTasks {
		// [][]*Task taskList are all tasks in the same application
		for index2, _ := range acceptedTasks[index] {
			var taskCurrentHeadNode data.MutexableNodeI
			templateNode := acceptedTasks[index][index2][0].HeadNode
			if templateNode.CurrentHeadNode != nil {
				taskCurrentHeadNode = templateNode.CurrentHeadNode
			} else {
				taskCurrentHeadNode = templateNode
			}
			arc, reverseArc := build.BuildArcWithMutex(taskCurrentHeadNode, machineNode)
			for index3,_ := range acceptedTasks[index][index2] {
				build.UpdateTaskToMachineCost(acceptedTasks[index][index2][index3].Index, machineNode.Machine.Index, 0)  // updata TaskToMachineCost cost
			}
			build.AddArcTONodes(arc, false)
			build.AddArcTONodes(reverseArc, true)

		}

	}
}

func (build *MapBuilder) BuildConstraints(acceptedTasks [][][]*data.Task){
	for index,_ := range acceptedTasks {
		// [][]*Task taskList are all tasks in the same application
		for index2, _:= range acceptedTasks[index] { // for the same task collection in one application
			var taskCurrentHeadNode data.MutexableNodeI
			templateNode := acceptedTasks[index][index2][0].HeadNode
			if templateNode.CurrentHeadNode != nil {
				taskCurrentHeadNode = templateNode.CurrentHeadNode
			}else {
				taskCurrentHeadNode = templateNode
			}
			for len(taskCurrentHeadNode.GetNotMutexTask()) != 0 { // if this node still has not-mutexed task.
				var taskCurrentHeadNode2 data.MutexableNodeI
				templateNode2 := taskCurrentHeadNode.GetNotMutexTask()[0].HeadNode
				if templateNode2.CurrentHeadNode != nil {
					taskCurrentHeadNode2 = templateNode2.CurrentHeadNode
				}else {
					taskCurrentHeadNode2 = templateNode2
				}
				taskCurrentHeadNode, _ = build.BuildMutex(taskCurrentHeadNode, taskCurrentHeadNode2)  // mutex these two node
			}
		}

	}
}


func (build *MapBuilder) BuildTemplate(){
	acceptedTasks := data.ApplicationAndTask // all node share the same templateNode
	// mutex all same tasks in the same application
	for index,_ := range acceptedTasks {
		// [][]*Task taskList are all tasks in the same application
		for index2, _:= range acceptedTasks[index] { //
			// []*Task tasks are all same tasks
			templateNode := build.BuildTemplateNode(make([]*data.Task, 0))
			for index3, _:= range acceptedTasks[index][index2] {
				templateNode.SourceTasks = append(templateNode.SourceTasks, acceptedTasks[index][index2][index3])
				arc, reverseArc := build.BuildArc(acceptedTasks[index][index2][index3].Tasknode, templateNode) // connect the taskNode to this mutexNode
				build.AddArcTONodes(arc, false)
				build.AddArcTONodes(reverseArc, true)
				acceptedTasks[index][index2][index3].UpdateHeadNode(templateNode)
			}
		}

	}


}

/**
	except for beginning, inter taskNode mutex, all other mutex is between caseNodes
 */
func (build *MapBuilder) BuildMutex(o data.MutexableNodeI, t data.MutexableNodeI) (*data.CaseNode, *data.CaseNode){

	mutexNode := build.BuildMutexNode(make([]*data.Task, 0))

	caseNode1 := build.BuildCaseNode(make([]*data.Task, 0))  // this for o node
	mutexNode.MutexTaskList = append(mutexNode.MutexTaskList, o.GetSourceTask()...)
	caseNode1.SourceTasks = o.GetSourceTask()  // all the caseNode has the same source, has the same sourceTasks
	caseNode1.TaskNotMutex = build.SubtractList(o.GetNotMutexTask(), t.GetSourceTask())
	caseNode1.SourceTasks[0].HeadNode.CurrentHeadNode = caseNode1

	caseNode2 := build.BuildCaseNode(make([]*data.Task, 0))  // this for t node
	mutexNode.MutexTaskList = append(mutexNode.MutexTaskList, t.GetSourceTask()...)
	caseNode2.SourceTasks = t.GetSourceTask()  // all the caseNode has the same source, has the same sourceTasks
	caseNode2.TaskNotMutex = build.SubtractList(t.GetNotMutexTask(), o.GetSourceTask())
	caseNode2.SourceTasks[0].HeadNode.CurrentHeadNode = caseNode2


	arc1, reverseArc1 := build.BuildArcWithMutex(o, mutexNode)
	build.AddArcTONodes(arc1, false)
	build.AddArcTONodes(reverseArc1, true)
	arc2, reverseArc2 := build.BuildArcWithMutex(t, mutexNode)
	build.AddArcTONodes(arc2, false)
	build.AddArcTONodes(reverseArc2, true)
	arc3, reverseArc3 := build.BuildArc(mutexNode, caseNode1)
	build.AddArcTONodes(arc3, false)
	build.AddArcTONodes(reverseArc3, true)
	arc4, reverseArc4 := build.BuildArc(mutexNode, caseNode2)
	build.AddArcTONodes(arc4, false)
	build.AddArcTONodes(reverseArc4, true)

	return caseNode1, caseNode2
}


/**
	create a new slice and substract
 */
func (build *MapBuilder) SubtractList(es []*data.Task, ed []*data.Task) []*data.Task{
	result := make([]*data.Task, len(es))
	copy(result, es)
	edmap := make(map[*data.Task] bool)
	for index, _ := range ed {
		edmap[ed[index]] = true
	}
	isSubed := 0
	for i, _ := range result{
		if _, ok := edmap[result[i-isSubed]]; ok{
			result = append(result[:i-isSubed], result[i+1-isSubed:]...)
			isSubed++
		}
	}

	return result
}

func (build *MapBuilder) UpdateTaskToMachineCost(taskIndex int, machineIndex int, cost int) {
	data.TaskToMachineCost[taskIndex][machineIndex] = cost
}