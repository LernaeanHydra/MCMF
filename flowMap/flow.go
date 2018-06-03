package flowMap

import (
	"container/list"
	"MCMF/data"
	"strconv"
	"errors"
)

type MapSolverI interface {
	GetMCMF()
	GetSPFA()
	Regret()
	UpdateMap()
}


type MapSolver struct {
	// this struct only has methods
}

func (solver *MapSolver)UpdateScheduledMachineTaskMap(task *data.Task, machineNode *data.MachineNode, arcs [][]int, i int, isAdded bool){
	if isAdded {
		for _, tag := range task.ExclusiveTag {
			if _, ok := machineNode.ScheduledTasks[tag]; ok { // if this tag has exsited
				machineNode.ScheduledTasks[tag] = append(machineNode.ScheduledTasks[tag], arcs[i][len(arcs[i])-1]^1)
			}else{ // if this tag hasn't exsited
				indexList := make([]int, 0)
				indexList = append(indexList, arcs[i][len(arcs[i])-1]^1)
				machineNode.ScheduledTasks[tag] = indexList
			}
		}
	}else { // we need to remove all
		arcIndex := arcs[i][0]
		for _, tag := range task.ExclusiveTag {
			if _, ok := machineNode.ScheduledTasks[tag]; ok { // if this tag has exsited
				for index,_ := range machineNode.ScheduledTasks[tag] {
					if machineNode.ScheduledTasks[tag][index] == arcIndex{
						machineNode.ScheduledTasks[tag] = append(machineNode.ScheduledTasks[tag][:index], machineNode.ScheduledTasks[tag][index+1:]...)
						break
					}
				}
			}else{ // if this tag hasn't exsited
				panic("this machine must has this tag before the task removed")
			}
		}
	}

}

func (solver *MapSolver)UpdateCapacityOfMachine(machineNode data.NodeI, pathFlow []int, isAdded bool){
	if isAdded {
		machineNode.GetRightOutArcs()[0].SubCapacity(pathFlow)
		machineNode.GetRightInArcs()[0].AddCapacity(pathFlow)
	}else {
		machineNode.GetRightOutArcs()[0].AddCapacity(pathFlow)
		machineNode.GetRightInArcs()[0].SubCapacity(pathFlow)
		// update start machine's ScheduledTasks map
	}

}

func (solver *MapSolver)UpdateCapacityOfPath(arcs [][]int, i int, pathFlow []int){
	for _,indexOfArcList := range arcs[i] {
		arc := data.ArcList[indexOfArcList]
		arc.SubCapacity(pathFlow)
		reverseArc := data.ArcList[indexOfArcList^1]
		reverseArc.AddCapacity(pathFlow)
	}

}

/**
	add newFlow to sum of FLow
 */
func (solver *MapSolver)AddFlow(flowSum []int, newFlow []int) []int{
	for i, _ := range flowSum{
		flowSum[i] += newFlow[i]
	}
	return flowSum
}

/**
	calculate the sum of multi-dimension flow cost
 */
func (solver *MapSolver)GetCost(cost int, newFlow []int) int{
	sum := 0
	for i, _ := range newFlow{
		sum += cost*newFlow[i]
	}
	return sum
}


/**
	this func check if all arc has enough capacity for task scheduled. except for the arc from machine to end Node
 */
func (solver *MapSolver)HasScheduled(arc *data.Arc) bool{
	for _, cap := range arc.Capacity {
		if cap > 0 {  // if dstNode is not machineNode, we can assume that if capacity > 0, then this task still not scheduled
			return false
		}
	}
	return true
}


/**
	this fucn check if machine has enough resource for task the task scheduled. when HasScheduled(arcToMachine) return false, this func will run
 */
func (solver *MapSolver)HasCapacity(arcToMachine *data.Arc, arcToEnd *data.Arc) bool{
	if !solver.HasScheduled(arcToMachine) {
		for i, _:= range arcToMachine.Capacity {  // if one dimension of resource not has enough resource, the task can't be scheduled to this machine
			if arcToMachine.Capacity[i] > arcToEnd.Capacity[i] {
				return false
			}
		}
		return true
	}else {
		panic("this task has been scheduled, we can't scheduled it twice")
	}

}


/*
	1.find all flow from start to end, start and end are indexes of NodeList. return all counter of two nodes connection; return the min cost and maxFlow
 */
func (solver *MapSolver)GetMCMF(s int, t int)(map[string]int, int, []int){
	result := make(map[string]int)
	cost := 0
	flow := make([]int, 0)

	for true {
		if isExsit, newCost, paths, arcs :=solver.GetSPFA(s, t); isExsit {
			// paths[0] must start from startNode and after 0, all path start from machine Node.
			var newFlow []int

			// all paths ends at machineNode
			// first path start from applicationNode
			for i, path := range paths{
				if(i == 0){
					// get the task of this path
					node := data.NodeList[path[1]] // taskNode
					taskNode, ok := node.(*data.TaskNode)
					if !ok {
						panic("NodeList[paths[0][2]] must be *TaskNode type")
					}
					task := taskNode.Task
					// get flow of this path
					pathFlow := data.NodeList[path[1]].GetLeftInArcs()[0].Capacity  // the flow of this task

					newFlow = pathFlow

					// sub capacity of the arc on this path
					solver.UpdateCapacityOfPath(arcs, i, pathFlow)

					// we need to update the arc's capacity. from this machine to end node, we need to sub it with pathFlow
					machineNode, ok := data.NodeList[path[len(path)-1]].(*data.MachineNode)
					if !ok {
						panic("data.NodeList[path[len(path)-1]] must be *MachineNode type")
					}
					// update capacity of arc from this machine node to end node
					solver.UpdateCapacityOfMachine(machineNode, pathFlow, true)

					// update machine's ScheduledTasks map
					solver.UpdateScheduledMachineTaskMap(task, machineNode, arcs, i, true)
				}else {
					// path[i] (i > 0) all start from machine node, end at another machine node. and the second node is the template node,
					startMachineNode, ok := data.NodeList[path[0]].(*data.MachineNode)
					if !ok {
						panic("data.NodeList[paths[i][0]] must be *MachineNode type if i > 0")
					}
					endMachineNode, ok := data.NodeList[path[len(path)-1]].(*data.MachineNode)
					if !ok {
						panic("data.NodeList[path[len(path)-1]] must be *MachineNode type")
					}

					templateNode, ok := data.NodeList[path[1]].(*data.TemplateNode) // get template node
					if !ok {
						panic("data.NodeList[paths[i][1]] must be *TemplateNode type if i > 0")
					}

					// get task
					task := templateNode.SourceTasks[0]

					// get flow of this path
					pathFlow := data.NodeList[path[1]].GetRightInArcs()[0].Capacity

					// update the start machine's capacity after regret
					solver.UpdateCapacityOfMachine(startMachineNode, pathFlow, false)

					// update start machine's ScheduledTasks map
					solver.UpdateScheduledMachineTaskMap(task, startMachineNode, arcs, i, false)

					solver.UpdateCapacityOfPath(arcs, i, pathFlow)

					// we need to update the arc's capacity. from this machine to end node, we need to sub it with pathFlow
					solver.UpdateCapacityOfMachine(endMachineNode, pathFlow, true)

					// update start machine's ScheduledTasks map
					solver.UpdateScheduledMachineTaskMap(task, endMachineNode, arcs, i, true)
				}
			}
			flow = solver.AddFlow(flow, newFlow)
			cost += solver.GetCost(newCost, newFlow)
			// take apart of paths, add all of them to result
			for _, path := range paths {
				var preNodeIndexStr string
				for i, nodeIndex := range path {
					if i == 0 {
						preNodeIndexStr = strconv.Itoa(nodeIndex)
						continue
					}else {
						postNodeIndexStr := strconv.Itoa(nodeIndex)
						key := preNodeIndexStr+"-"+postNodeIndexStr
						if _, ok :=result[key]; ok {
							result[key] ++
						}else{
							result[key] = 1
						}
						preNodeIndexStr = postNodeIndexStr

					}
				}
			}


		}else {
			break
		}
	}


	return result, cost, flow
}

/**
	1.find one augmenting-path that has the min cost. for regret, there may be mutipath need to be regret
	2.give the start and end, and return whether there is a augmenting-path, and return all paths presenting by node ID
		and the lists of Index of ArcList that has this Node as its destination node in paths[i]. paths[0] start from startNode, end at machineNode.
		after that, all paths start from machineNode, end at another machineNode
	3. len(arcs[i]) == len(paths[i])-1
*/
func (solver *MapSolver)GetSPFA(s int, t int)(bool, int, [][]int, [][]int){
	gPre := make([][]int, len(data.NodeList))  // initialize gPre
	for i:=0; i<len(gPre); i ++ {
		list := make([]int, 0)
		list = append(list, -1)  // default -1
		gPre[i] = list
	}

	gPath := make([][]int, len(data.NodeList))  // initialize gPath
	for i:=0; i<len(gPath); i ++ {
		list := make([]int, 0)
		list = append(list, -1)  // default -1
		gPath[i] = list
	}

	gDist := make([]int, len(data.NodeList)) // initialize gDist
	for i:=0; i<len(gDist); i ++ {
		gDist[i] = data.MAXINTVALUE  // default INT_MAX
	}

	gIsRegretted := make(map[int][]*data.Arc, 0) // all key is the index of the machineNode in NodeList.
												// tell which arcs should be regretted by the min cost task

	gDist[s] = 0
	Q := list.New()  // Q is the loose queue, it record all node , from which the min cost to another node may change
	Q.PushBack(s)
	for Q.Len() > 0  {
		nodeIndexElement := Q.Front()
		Q.Remove(nodeIndexElement)
		nodeIndex, err := nodeIndexElement.Value.(int)
		if(!err){
			panic(err)
		}
		node := data.NodeList[nodeIndex]
		_, isMachineNode := node.(*data.MachineNode)  // if srcNode is machineNode
		var outArcList []*data.Arc
		if arcList, ok := gIsRegretted[node.GetID()]; ok{ // if node is machineNode, and this machineNode's min cost task is mutexed with it now
			outArcList = arcList
		} else {
			outArcList = node.GetRightOutArcs()
		}
		for i, _ := range outArcList {
			toNode := outArcList[i].DstNode
			_, ok := toNode.(*data.MachineNode)  // check if dstNode is machineNode
			toNodeIndex := toNode.GetID()
			_, isEndNode := toNode.(*data.MetaNode)

			if !isEndNode && (solver.HasScheduled(outArcList[i]) || gDist[nodeIndex]+outArcList[i].Cost >= gDist[toNodeIndex]) { // if no capacity or more cost
				continue
			}
			// if toNode is EndNode, the arc must has enough capacity, and cost must be less or be equal
			if isEndNode && gDist[nodeIndex]+outArcList[i].Cost == gDist[toNodeIndex] { // if dstNode is EndNode, we need to store all path that can get min cost
				gPre[toNodeIndex] = append(gPre[toNodeIndex], nodeIndex)
				gPath[toNodeIndex] = append(gPath[toNodeIndex], outArcList[i].ID)
				continue
			}

			if isEndNode && gDist[nodeIndex]+outArcList[i].Cost < gDist[toNodeIndex] { // if dstNode is EndNode, we need to store all path that can get min cost
				gDist[toNodeIndex] = gDist[nodeIndex] + outArcList[i].Cost
				gPre[toNodeIndex][0] = nodeIndex
				gPath[toNodeIndex][0] = outArcList[i].ID
				continue
			}

			// if !solver.HasScheduled(outArcList[i]) && gDist[nodeIndex]+outArcList[i].Cost < gDist[toNodeIndex]
			if !isMachineNode && ok { // if dstNode is machineNode, we need to calculate the cost of scheduling this task to this machine(include mutexing and preemption)
				// check if there is other tasks existed in the machine, which is mutexed with the task
				templateNode, ok2 := node.(*data.TemplateNode)
				if !ok2 {
					panic("machineNode's preNode must be templateNode")
				}
				exclusiveTaskTagList := templateNode.GetSourceTask()[0].ExclusiveTag
				machineNode := toNode.(*data.MachineNode)
				mutexArcList := solver.GetMutexArcList(exclusiveTaskTagList, machineNode)  // if mutexed, get all mutexed arc
				if mutexArcList != nil { // if this task mutex with the tasks previous scheduled on this machine
					// check if capacity is enough
					// if enough, update gIsMutexed; if not, leave it left
					sumCapacity := solver.GetCapacityAfterMutex(machineNode, mutexArcList)
					// then check if capacity is enough for task required, if not run preemption.
					var regretArcList []int
					if !solver.HasEnoughCapacityForTask(sumCapacity, templateNode.GetSourceTask()[0]) { // get arc list for preemption
						var err error
						regretArcList, err = solver.GetPreemptArcList(machineNode, mutexArcList, sumCapacity, templateNode.GetSourceTask()[0]) // get preemption arc list for capacity
						if err != nil {
							 // drop this path
							 continue
						}
					}else {
						regretArcList =	mutexArcList
					}
					// get regreting and scheduling cost
					arcCost := outArcList[i].Cost // the cost for arc between templateNode and machienNode
					cost, costForReverseArcList := solver.GetCostByRegretting(regretArcList, machineNode, arcCost) // get min cost for regretting
					if cost < gDist[data.EndNode.GetID()] {  // if the cost is less than current min cost of EndNode
						// 1. update machine cost, pre, path
						// 2. update reverseArc cost
						// 3. update gIsRegret with regretArcList
						// 4. pushback
						gDist[toNodeIndex] = cost
						gPre[toNodeIndex][0] = nodeIndex
						gPath[toNodeIndex][0] = outArcList[i].ID
						isRegrettedArcList := make([]*data.Arc, 0)
						for i,_ := range regretArcList {
							data.ArcList[regretArcList[i]].Cost = costForReverseArcList[i]
							isRegrettedArcList = append(isRegrettedArcList, data.ArcList[regretArcList[i]])
						}
						gIsRegretted[toNodeIndex] = isRegrettedArcList

						Q.PushBack(toNodeIndex)
					}else{
						continue // drop it
					}
				}else { // if not mutex. check its capacity, if enough add this machineNode to Stack, then delete that map key. or get preemption task list, and update preemption-task list to map
					var cost int
					var regretArcList []int
					var costForReverseArcList []int
					if !solver.HasEnoughCapacityForTask(toNode.GetRightOutArcs()[0].Capacity, templateNode.GetSourceTask()[0]) { // if not has enough capacity, then get arc list for preemption
						var err error
						regretArcList, err = solver.GetPreemptArcList(machineNode, mutexArcList, toNode.GetRightOutArcs()[0].Capacity, templateNode.GetSourceTask()[0]) // get preemption arc list for capacity
						if err != nil {
							// drop this path
							continue
						}
						arcCost := outArcList[i].Cost // the cost for arc between templateNode and machienNode
						cost, costForReverseArcList = solver.GetCostByRegretting(regretArcList, machineNode, arcCost) // get min cost for regretting
					} else { // if capacity is enough
						cost = outArcList[i].Cost
					}

					if cost < gDist[data.EndNode.GetID()] {
						gDist[toNodeIndex] = cost
						gPre[toNodeIndex][0] = nodeIndex
						gPath[toNodeIndex][0] = outArcList[i].ID
						Q.PushBack(toNodeIndex)
						if regretArcList == nil {
							delete(gIsRegretted, toNodeIndex) // if it has, delete it . if not, leave it
						}else {
							isRegrettedArcList := make([]*data.Arc, 0)
							for i,_ := range regretArcList {
								data.ArcList[regretArcList[i]].Cost = costForReverseArcList[i]
								isRegrettedArcList = append(isRegrettedArcList, data.ArcList[regretArcList[i]])
							}
							gIsRegretted[toNodeIndex] = isRegrettedArcList
						}

					}else {
						continue
					}
				}
			}else { // if dstNode is not machineNode, we update it if it has capacity and has less cost
				gDist[toNodeIndex] = gDist[nodeIndex] + outArcList[i].Cost
				gPre[toNodeIndex][0] = nodeIndex
				gPath[toNodeIndex][0] = outArcList[i].ID
				Q.PushBack(toNodeIndex)
			}
		}
	}
	if gDist[data.EndNode.GetID()] != data.MAXINTVALUE {
		nodeResultList := make([][]int, 0)
		arcResultList := make([][]int, 0)
		pathNum := len(gPre[data.EndNode.GetID()])
		if pathNum < 1 {
			panic("gPre[EndNOde] should have value")
		}
		for i:=0; i<pathNum; i++ {
			nodeResult := make([]int, 0)
			nodeIndex := gPre[data.EndNode.GetID()][i] // it all start with MachineNode, end with startNode

			arcResult := make([]int, 0)
			arcIndex := gPath[nodeIndex][0]

			for gPre[nodeIndex][0] != -1 {
				tmp := append([]int{}, nodeIndex)
				nodeResult = append(tmp, nodeResult...)
				nodeIndex = gPre[nodeIndex][0]

				tmp2 := append([]int{}, arcIndex)
				arcResult = append(tmp2, arcResult...)
				arcIndex = gPath[nodeIndex][0]
			}
			// it dosen't contain startNode
			nodeResultList = append(nodeResultList, nodeResult)
			arcResultList = append(arcResultList, arcResult)


		}

		if len(nodeResultList) == 0 {
			panic("there should be one Node path")
		}else if len(nodeResultList) == 1{
			// 1. there is no regret
			// 2. there is only one regret
			// check reverse 3rd node is machineNode ?
			node:= data.NodeList[nodeResultList[0][len(nodeResultList[0])-3]]
			_, isMachineNode := node.(*data.MachineNode)
			if isMachineNode {
				nodeResultList = append(nodeResultList, nodeResultList[0][len(nodeResultList[0])-3:])
				nodeResultList[0] = nodeResultList[0][:len(nodeResultList[0])-2]

				arcResultList = append(arcResultList, arcResultList[0][len(arcResultList[0])-2:])
				arcResultList[0] = arcResultList[0][:len(arcResultList[0])-2]
			}
		}else {
			// 1. there is more than one regret
			// 2. delete pre part for every path, and add pre part for new path 0
			nodeResultList = append(nodeResultList, nodeResultList[0][len(nodeResultList[0])-3:])
			for i:=1; i<len(nodeResultList)-1; i++ {
				nodeResultList[i] = nodeResultList[0][len(nodeResultList[0])-3:]
			}
			nodeResultList[0] = nodeResultList[0][:len(nodeResultList[0])-2]

			arcResultList = append(arcResultList, arcResultList[0][len(arcResultList[0])-2:])
			for i:=1; i<len(arcResultList)-1; i++ {
				arcResultList[i] = arcResultList[0][len(arcResultList[0])-2:]
			}
			arcResultList[0] = arcResultList[0][:len(arcResultList[0])-2]

		}

		return true, gDist[data.EndNode.GetID()], nodeResultList, arcResultList
	}
	return false, 0, nil, nil


}

func (solver *MapSolver)GetAllOutArcs(node data.NodeI)([]*data.Arc){
	leftOutArcs := node.GetLeftOutArcs()
	rightOutArcs := node.GetRightOutArcs()
	arcs := make([]*data.Arc, 0)
	arcs = append(arcs, leftOutArcs...)
	arcs = append(arcs, rightOutArcs...)
	return arcs
}

func (solver *MapSolver)HasEnoughCapacityForTask(capacity []int, task *data.Task) bool{
	for i,_ := range capacity {
		if i == 0{
			if capacity[i] < task.Cpu {
				return false
			}
		}else if i == 1{
			if capacity[i] < task.Mem {
				return false
			}
		}else if i == 2{
			if capacity[i] < task.DiskSize {
				return false
			}
		}else if i == 3{
			var instanceNum int
			if task.IsKeyInstance {
				instanceNum = 1
			}else {
				instanceNum = 0
			}

			if capacity[i] < instanceNum{
				return false
			}
		}
	}
	return true
}


/**
	when a task want to be scheduled to the machine, we need to get the leftOut arc list for mutexing. if not mutexed, we get nil
 */
func (solver *MapSolver)GetMutexArcList(exclusiveTaskTagList []string, machineNode *data.MachineNode)([]int){
	mutexArcList := make([]int,0)
	for _, tag := range exclusiveTaskTagList{
		if list, ok3 := machineNode.ScheduledTasks[tag]; ok3 { // existed
			for _, arcIndex := range list {
				isContained := false
				for k,_ := range mutexArcList {
					if mutexArcList[k] == arcIndex {
						isContained = true
						break
					}
				}
				if !isContained {
					mutexArcList = append(mutexArcList, arcIndex)
				}
			}
		}
	}
	if len(mutexArcList) == 0 {
		return nil
	}
	return mutexArcList
}

/**
	if machineNode capacity is not enough for the task. we need to preempt for it, so we need to get preemptionList for min cost
	if there is no space for task to preempt. give a error. and we drop this task this schedule path
 */
func (solver *MapSolver)GetPreemptArcList(node *data.MachineNode, mutexArcList []int, capacity []int, task *data.Task)([]int, error){
	var resultArcList []int
	if mutexArcList == nil{
		resultArcList = make([]int, 0)
	}else {
		resultArcList = mutexArcList
	}

	requiredCapacity := make([]int, len(capacity))
	for i,_ := range capacity {
		if i == 0{
			requiredCapacity[i] = task.Cpu - capacity[i]
		}else if i == 1{
			requiredCapacity[i] = task.Mem - capacity[i]
		}else if i == 2{
			requiredCapacity[i] = task.DiskSize - capacity[i]
		}else if i == 3{
			var instanceNum int
			if task.IsKeyInstance {
				instanceNum = 1
			}else {
				instanceNum = 0
			}

			requiredCapacity[i] = instanceNum - capacity[i]
		}
	}

	for i:=0; i<len(requiredCapacity); i++ {
		if requiredCapacity[i] <= 0 {
			continue
		}
		for _, arc := range node.GetLeftOutArcs(){
			isContained := false
			for _, arcId := range resultArcList {
				if arc.ID == arcId {
					isContained = true
					break
				}
			}
			if isContained {
				continue
			}

			if arc.Capacity[i] > 0{
				for j :=0; j<len(requiredCapacity); j++ {
					requiredCapacity[j] -= arc.Capacity[j]
				}
				resultArcList = append(resultArcList, arc.ID)
				isEnough := true
				for j :=0; j<len(requiredCapacity); j++ {
					if requiredCapacity[j] > 0 {
						isEnough = false
					}
				}
				if isEnough {
					return resultArcList, nil
				}
				if requiredCapacity[i] <= 0{
					break
				}
			}else {
				continue
			}
		}
		if requiredCapacity[i] > 0 {
			return nil, errors.New("there is no task can be regretted for enough capacity for the task required")
		}
	}

	return nil, nil  // it can't run
}

/**
	where should we schedule tasks that preempted or mutexed by the task and we can get min cost, and preemption cost list for every arc
 */
func (solver *MapSolver)GetMinCostAfterRegreting(node *data.MachineNode, regretArcList []int)(int, []int){

	
	return  0, nil
}

/**
	if capacity is not enough, we need to preempt, this func get the min cost of preemption so that this task can be scheduled to the machine
 */
func (solver *MapSolver)GetCostByRegretting(regretArcList []int, machineNode *data.MachineNode, cost int)(int, []int){
	regrettingCost, costForReverseArcList := solver.GetMinCostAfterRegreting(machineNode, regretArcList)
	cost += regrettingCost
	return cost, costForReverseArcList
}

/**
	if machineNode has tasks scheduled that mutexed with the task. we need to regret it, so we need to get min cost of regretting
 */
func (solver *MapSolver)GetCapacityAfterMutex(toNode *data.MachineNode, regretArcList []int)([]int){
	if len(regretArcList) == 0 {
		panic("regretArcList shouldn't be empty")
	}
	sumCapacity := data.ArcList[regretArcList[0]].Capacity
	for mutexArcIndex := 1; mutexArcIndex<len(regretArcList); mutexArcIndex++ { // the arc in the mutexArcList must has capacity
		sumCapacity = solver.AddFlow(sumCapacity, data.ArcList[regretArcList[mutexArcIndex]].Capacity)
	}
	sumCapacity = solver.AddFlow(sumCapacity, toNode.GetRightOutArcs()[0].Capacity)
	return sumCapacity
}