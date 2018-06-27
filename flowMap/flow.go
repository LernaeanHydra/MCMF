package flowMap

import (
	"container/list"
	"MCMF/data"
	"strconv"
	"errors"
	"fmt"
	"time"
)

type MapSolverI interface {
	GetMCMF()
	GetSPFA()
	Regret()
	UpdateMap()
}


type MapSolver struct {

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
	}else { // we need to remove all, however, when regret process is on, may there is no mutex tag need to remove.
		arcIndex := arcs[i][0] // reverse arc Id
		// for test
		//if machineNode.GetID() == 661 && len(arcs) == 2{
		//	fmt.Print("for test")
		//}
		///////
		for _, tag := range task.ExclusiveTag {
			if _, ok := machineNode.ScheduledTasks[tag]; ok { // if this tag has exsited
				for index,_ := range machineNode.ScheduledTasks[tag] {
					if machineNode.ScheduledTasks[tag][index] == arcIndex{
						machineNode.ScheduledTasks[tag] = append(machineNode.ScheduledTasks[tag][:index], machineNode.ScheduledTasks[tag][index+1:]...)
						if len(machineNode.ScheduledTasks[tag]) == 0{
							delete(machineNode.ScheduledTasks, tag)  // if this tag has no task, delete it
						}
						break
					}
				}

			}
			/** todo there sometimes need to remove the task's tag, which is not on the machine. this problem hasn't been solved yet **/
			//else{ // if this tag hasn't exsited
			//	panic("this machine must has this tag before the task removed")
			//}
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
	result := make(map[string]int) // there is how many srcNode-dstNode? the distance between srcNode and dstNode is 1
	cost := 0
	flow := make([]int, data.RESOURCEDIMENSION)

	counter := 0
	for true {
		counter ++
		fmt.Println(counter)
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

					pathFlow := data.NodeList[path[1]].GetLeftInArcs()[0].CopyCapacity()  // the flow of this task.

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
					pathFlow := task.GetCapacity()

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
			fmt.Println(paths)
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
	// defer solver.timeCost("GetSPFA", time.Now())
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

	gDist[s] = 0
	Q := list.New()  // Q is the loose queue, it record all node , from which the min cost to another node may change
	Q.PushBack(s)
	cont := 0
	needNoRegret := false  // this is only for templateNode --> machineNode, if this is true, mean that there is no need to iterate last outArcList
	for Q.Len() > 0  {
		nodeIndexElement := Q.Front()
		Q.Remove(nodeIndexElement)
		nodeIndex, err := nodeIndexElement.Value.(int)
		if(!err){
			panic(err)
		}
		node := data.NodeList[nodeIndex]
		// check if node is template node
		// if it's template node, we get arcs by TaskMinCostArcIdList's order.
		// iterate until there is no preemption happened.
		var outArcList []*data.Arc

		if templNode, ok := node.(*data.TemplateNode); ok {
			needNoRegret = false
			outArcList = data.TaskMinCostArcIdList[templNode.SourceTasks[0].Index]

		}else if applicationNode, ok := node.(*data.ApplicationNode); ok{
			// find the first unscheduled taskNode
			for i, arc := range applicationNode.GetRightOutArcs()  {
				if solver.HasScheduled(arc) {
					continue
				}
				outArcList = []*data.Arc{applicationNode.GetRightOutArcs()[i]}
			}
		}else {
			outArcList = node.GetRightOutArcs() // get right out arcs for iteration
		}

		preemptMachineNum := 0 // we assume that if we preempt 10 min cost machine, we can almost get the min cost. after that we only need to find no regret machine
		//  findNoPreemptionNode := false  不知道这个是用来干嘛的
		for i, _ := range outArcList {
			toNode := outArcList[i].DstNode
			_, ok := toNode.(*data.MachineNode)  // check if dstNode is machineNode
			toNodeIndex := toNode.GetID()

			if solver.HasScheduled(outArcList[i]) || gDist[nodeIndex]+outArcList[i].Cost >= gDist[toNodeIndex] { // if no capacity or more cost
				continue

			}

			// if !solver.HasScheduled(outArcList[i]) && gDist[nodeIndex]+outArcList[i].Cost < gDist[toNodeIndex]
			if ok { // if dstNode is machineNode, we need to calculate the cost of scheduling this task to this machine(include mutexing and preemption)
				// for we iterate machine by min cost order, so we should stop if there is no preemption
				// check if there is other tasks existed in the machine, which is mutexed with the task
				templateNode, ok2 := node.(*data.TemplateNode)
				if !ok2 {
					panic("machineNode's preNode must be templateNode")
				}
				exclusiveTaskTagList := templateNode.GetSourceTask()[0].ExclusiveTag
				machineNode := toNode.(*data.MachineNode)
				mutexArcList := solver.GetMutexArcList(exclusiveTaskTagList, machineNode)  // if mutexed, get all mutexed arc
				if mutexArcList != nil { // if this task mutex with the tasks previous scheduled on this machine
					if preemptMachineNum >= 10{
						continue
					}
					preemptMachineNum ++
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
					cost, costForReverseArcList, arcIndexList, err := solver.GetCostByRegretting(regretArcList, machineNode, arcCost) // get min cost for regretting
					if err != nil{  //TODO if no suitable machine for preempted task. we now drop this path, however we can re-getpreemptionTask
						break
					}

					if cost < gDist[data.EndNode.GetID()] {  // if the cost is less than current min cost of EndNode, we need to updata the Dist, Pre, path on this path
						start := time.Now()
						// update endNode cost. remove all pre、path
						gDist[data.EndNode.GetID()] = cost
						gPre[data.EndNode.GetID()] = gPre[data.EndNode.GetID()][:0]
						gPath[data.EndNode.GetID()] = gPath[data.EndNode.GetID()][:0]
						gPre[toNodeIndex] = gPre[toNodeIndex][:0]
						gPath[toNodeIndex] = gPath[toNodeIndex][:0]

						// 1. update machine cost, pre, path
						// 2. update reverseArc cost
						// 3. update cost pre path on the path
						// 4. update endNode pre path
						gDist[toNodeIndex] = cost
						gPre[toNodeIndex] = append(gPre[toNodeIndex], nodeIndex)
						gPath[toNodeIndex] = append(gPath[toNodeIndex], outArcList[i].ID)

						for j,_ := range regretArcList { // update regret machine status
							machineNode := data.ArcList[arcIndexList[j]].DstNode.(*data.MachineNode)
							gPre[machineNode.GetID()] = gPre[machineNode.GetID()][:0]
							gPath[machineNode.GetID()] = gPath[machineNode.GetID()][:0]
						}
						preNode := toNode
						for j,_ := range regretArcList {
							// update reverseArc cost
							data.ArcList[regretArcList[j]].Cost = costForReverseArcList[j]
							// update templateNode
							tempNode := data.ArcList[regretArcList[j]].DstNode
							gDist[tempNode.GetID()] = gDist[preNode.GetID()] + data.ArcList[regretArcList[j]].Cost
							gPre[tempNode.GetID()][0] = preNode.GetID()
							gPath[tempNode.GetID()][0] = regretArcList[j]

							// update machineNode, we need to append it.
							machineNode :=  data.ArcList[arcIndexList[j]].DstNode.(*data.MachineNode)
							gDist[machineNode.GetID()] = cost
							gPre[machineNode.GetID()] = append(gPre[machineNode.GetID()], tempNode.GetID())
							gPath[machineNode.GetID()] = append(gPath[machineNode.GetID()],  arcIndexList[j])

							// update endNode, the machine node might be appended previously,so we need to avoid appending muti-times
							// if pre has two src nodes, we can judge that the endNode has append this machineNode
							if len(gPre[machineNode.GetID()]) == 1 {
								gPre[data.EndNode.GetID()] = append(gPre[data.EndNode.GetID()], machineNode.GetID())
								gPath[data.EndNode.GetID()] = append(gPath[data.EndNode.GetID()], machineNode.GetRightOutArcs()[0].ID)
							}
						}
						terminal :=time.Since(start)
						fmt.Println("updatePath: "+terminal.String())
					}else{
						continue // drop it
					}
				}else { // if not mutex. check its capacity, if enough add this machineNode to Stack, then delete that map key. or get preemption task list, and update preemption-task list to map
					var cost int
					var regretArcList []int
					var arcIndexList []int
					var costForReverseArcList []int
					if !solver.HasEnoughCapacityForTask(toNode.GetRightOutArcs()[0].Capacity, templateNode.GetSourceTask()[0]) { // if not has enough capacity, then get arc list for preemption
						if preemptMachineNum >= 10{
							continue
						}
						preemptMachineNum ++
						var err error
						regretArcList, err = solver.GetPreemptArcList(machineNode, mutexArcList, toNode.GetRightOutArcs()[0].Capacity, templateNode.GetSourceTask()[0]) // get preemption arc list for capacity
						if err != nil {
							// drop this path
							continue
						}
						arcCost := outArcList[i].Cost // the cost for arc between templateNode and machienNode
						cost, costForReverseArcList, arcIndexList, err = solver.GetCostByRegretting(regretArcList, machineNode, arcCost) // get min cost for regretting
						if err != nil{  //TODO if no suitable machine for preempted task. we now drop this path, however we can re-getpreemptionTask
							break
						}
					} else { // if capacity is enough
						cost = outArcList[i].Cost
						needNoRegret = true  // no matter this machine's cost, we stop the iteration
					}

					if cost < gDist[data.EndNode.GetID()] {
						start := time.Now()
						// update endNode cost. remove all pre、path
						gDist[data.EndNode.GetID()] = cost
						gPre[data.EndNode.GetID()] = gPre[data.EndNode.GetID()][:0]
						gPath[data.EndNode.GetID()] = gPath[data.EndNode.GetID()][:0]
						// todo we also need to remove machineNode's(include scheduling machine and rescheduling machine ) pre and path
						gPre[toNodeIndex] = gPre[toNodeIndex][:0]
						gPath[toNodeIndex] = gPath[toNodeIndex][:0]

						// todo we need to append
						gDist[toNodeIndex] = cost
						gPre[toNodeIndex] = append(gPre[toNodeIndex], nodeIndex)
						gPath[toNodeIndex] = append(gPath[toNodeIndex], outArcList[i].ID)
						// if there is no regretList
						if regretArcList == nil { // if no arc need to be regret, we schedule the task to this machine

							gPre[data.EndNode.GetID()] = append(gPre[data.EndNode.GetID()], machineNode.GetID())
							gPath[data.EndNode.GetID()] = append(gPath[data.EndNode.GetID()], machineNode.GetRightOutArcs()[0].ID)
							break
						}else {
							// todo we need to remove all reschedule machine pre and path.
							for j,_ := range regretArcList {
								machineNode :=  data.ArcList[arcIndexList[j]].DstNode.(*data.MachineNode)
								gPre[machineNode.GetID()] = gPre[machineNode.GetID()][:0]
								gPath[machineNode.GetID()] = gPath[machineNode.GetID()][:0]
							}
							preNode := toNode
							for j,_ := range regretArcList {
								// update reverseArc cost
								data.ArcList[regretArcList[j]].Cost = costForReverseArcList[j]
								// update templateNode
								tempNode := data.ArcList[regretArcList[j]].DstNode
								gDist[tempNode.GetID()] = gDist[preNode.GetID()] + data.ArcList[regretArcList[j]].Cost
								gPre[tempNode.GetID()][0] = preNode.GetID()
								gPath[tempNode.GetID()][0] = regretArcList[j]

								// update machineNode, we need to append it.
								machineNode :=  data.ArcList[arcIndexList[j]].DstNode.(*data.MachineNode)
								gDist[machineNode.GetID()] = cost
								gPre[machineNode.GetID()] = append(gPre[machineNode.GetID()], tempNode.GetID())
								gPath[machineNode.GetID()] = append(gPath[machineNode.GetID()],  arcIndexList[j])

								// update endNode, the machine node might be appended previously,so we need to avoid appending muti-times
								// if pre has two src nodes, we can judge that the endNode has append this machineNode
								if len(gPre[machineNode.GetID()]) == 1 {
									gPre[data.EndNode.GetID()] = append(gPre[data.EndNode.GetID()], machineNode.GetID())
									gPath[data.EndNode.GetID()] = append(gPath[data.EndNode.GetID()], machineNode.GetRightOutArcs()[0].ID)
								}

							}
						}
						terminal :=time.Since(start)
						fmt.Println("updatePath: "+terminal.String())

					}else {
						if needNoRegret {
							break
						}
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
	cont++
	//fmt.Println(cont)
	if gDist[data.EndNode.GetID()] != data.MAXINTVALUE {
		nodeResultList := make([][]int, 0)
		arcResultList := make([][]int, 0)
		// todo pathNum should iterate gPre[data.EndNode.GetID()], and get the sum of the machines' pre branch

		for i, machineNodeId := range gPre[data.EndNode.GetID()]{
			for j, _ := range gPre[machineNodeId] {
				nodeResult := make([]int, 0)
				nodeIndex := gPre[data.EndNode.GetID()][i] // it all start with MachineNode, end with startNode
				tmp := append([]int{}, nodeIndex)
				nodeResult = append(tmp, nodeResult...)
				fmt.Println(nodeIndex)

				arcResult := make([]int, 0)
				arcIndex := gPath[nodeIndex][j]
				tmp2 := append([]int{}, arcIndex)
				arcResult = append(tmp2, arcResult...)

				nodeIndex = gPre[nodeIndex][j]
				arcIndex = gPath[nodeIndex][0]

				for gPre[nodeIndex][0] != -1 {
					fmt.Println(nodeIndex)
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


		}

		//fmt.Println(cont)
		if len(nodeResultList) == 0 {
			panic("there should be one Node path")
		}else if len(nodeResultList) == 1{
			// 1. there is no regret
			// 2. there is only one regret
			// check reverse 3rd node is machineNode ?
			node:= data.NodeList[nodeResultList[0][len(nodeResultList[0])-3]]
			_, isMachineNode := node.(*data.MachineNode)
			if isMachineNode { // if there is one regret, we need to split it, or we leave it as it be
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
				nodeResultList[i] = nodeResultList[i][len(nodeResultList[0])-3:]
			}
			nodeResultList[0] = nodeResultList[0][:len(nodeResultList[0])-2]

			arcResultList = append(arcResultList, arcResultList[0][len(arcResultList[0])-2:])
			for i:=1; i<len(arcResultList)-1; i++ {
				arcResultList[i] = arcResultList[i][len(arcResultList[0])-2:]
			}
			arcResultList[0] = arcResultList[0][:len(arcResultList[0])-2]

		}
		//fmt.Println(cont)
		return true, gDist[data.EndNode.GetID()], nodeResultList, arcResultList
	}
	fmt.Println("???")
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
	//defer solver.timeCost("GetMutexArcList", time.Now())
	mutexArcList := make([]int,0)
	for _, tag := range exclusiveTaskTagList{
		if list, ok3 := machineNode.ScheduledTasks[tag]; ok3 { // existed
			for i, arcIndex := range list { // arc index of ArcList
				// if this arc is added to mutexArcList
				isContained := false
				for k,_ := range mutexArcList {
					if mutexArcList[k] == arcIndex {
						isContained = true
						break
					}
				}
				// we need to assure that this arc has not been scheduled, and it's not an error.
				if solver.HasScheduled(data.ArcList[arcIndex]) {
					machineNode.ScheduledTasks[tag] = append(machineNode.ScheduledTasks[tag][:i], machineNode.ScheduledTasks[tag][i+1:]...)
					continue
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
	//defer solver.timeCost("GetPreemptArcList", time.Now())
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
		// we need to get leftOutArcs randomly, avoiding only preempting small part of arcs
		length := len(node.GetLeftOutArcs())
		startIndex := data.RandomArc.Intn(length)
		for j:=0; j<length; j++{
			arc := node.GetLeftOutArcs()[(j+startIndex)%length]

			if arc.Capacity[i] > 0{
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

				for j :=0; j<len(requiredCapacity); j++ {
					requiredCapacity[j] -= arc.Capacity[j]
				}
				resultArcList = append(resultArcList, arc.ID)
				isEnough := true
				for j :=0; j<len(requiredCapacity); j++ {
					if requiredCapacity[j] > 0 {
						isEnough = false
						break
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
func (solver *MapSolver)GetMinCostAfterRegreting(node *data.MachineNode, regretArcList []int)(int, []int, []int, error){
	if regretArcList == nil || len(regretArcList) == 0{
		panic("GetMinCostAfterRegreting's regretArcList can't be empty")
	}
	costSum := 0
	costList := make([]int, len(regretArcList))
	scheduledMachineIdList := make([]int, len(regretArcList))  // store machineNode id for each preemted task finally schedulation
	scheduledMachineArcList := make([]int, len(regretArcList))  // store arc index of data.ArcList for every task's templateNode to machienNode
	for i, _ := range scheduledMachineIdList{  // initial all task to -1 machine
		scheduledMachineIdList[i] = -1
	}
	for i, _:= range regretArcList {
		node := data.ArcList[regretArcList[i]].DstNode
		templateNode, err := node.(*data.TemplateNode)
		if !err {
			panic("regretArc's dstNode must be templateNode")
		}
		task := templateNode.SourceTasks[0]


		// we only need to iterate by TaskMinCostArcIdList's order
		// TODO we only need to iterate by TaskMinCostArcIdList's order
		minCost := data.MAXINTVALUE
		for _, arc := range data.TaskMinCostArcIdList[templateNode.SourceTasks[0].Index]{
			// the new machine can't be the old machine
			if(solver.HasScheduled(arc)){
				continue
			}
			targetNode := arc.DstNode
			targetMachineNode, err := targetNode.(*data.MachineNode)
			if !err {
				panic("templateNode can only connect to machineNode on its right")
			}
			j := targetMachineNode.Machine.Index
			// 1. the new machine has less cost
			// 2. the new machine has enough capacity
			// 3. the new machine can't be mutexed with task
			// check its capacity and its mutexed tag
			hasEnoughCapacity := false
			isMutexed := false

			toMachine := data.ClusterMachineList[j]
			toMachineNode := toMachine.Node

			// check if mutexed
			for _,tag := range task.ExclusiveTag{
				if _, isExsit := toMachineNode.ScheduledTasks[tag]; isExsit {
					isMutexed = true
					break
				}
			}
			if isMutexed {
				continue
			}
			// check if has enough capacity
			arcToEnd := toMachineNode.GetRightOutArcs()[0]
			sumCapacity := make([]int, len(arcToEnd.Capacity))
			data.ListAdd(sumCapacity, arcToEnd.Capacity)
			for k:=0; k<i; k++ { // check if this machine has been used for previous task schedulation
				if arc.DstNode.GetID() == scheduledMachineIdList[k] {
					node_ := data.ArcList[regretArcList[k]].DstNode
					templateNode_ := node_.(*data.TemplateNode)
					task_ := templateNode_.SourceTasks[0]
					data.ListSub(sumCapacity, task_.GetCapacity())
				}
			}
			if solver.HasEnoughCapacityForTask(sumCapacity, task){
				hasEnoughCapacity = true
			}
			if !hasEnoughCapacity{
				continue
			}

			// update minCost and scheduledMachineList

			scheduledMachineIdList[i] = arc.DstNode.GetID()
			scheduledMachineArcList[i] = arc.ID
			minCost = arc.Cost
			break

		}
		if minCost == data.MAXINTVALUE{  // there is no machine for this task to be scheduled, we droped this task(or we can get another preemptedArcList)
			return 0, nil, nil, errors.New("no machine suitable for preempted task")
		}
		costList[i] = -minCost

	}
	// get sumCost
	for i,_ := range regretArcList {
		costSum += (data.ArcList[regretArcList[i]].Cost - costList[i])
	}


	return  costSum, costList, scheduledMachineArcList, nil
}

/**
	if capacity is not enough, we need to preempt, this func get the min cost of preemption so that this task can be scheduled to the machine
	this func also tell us the cost of the reverseArc from machineNode to templateNode. and give the arcIndex of rightOutArc of templateNode
 */
func (solver *MapSolver)GetCostByRegretting(regretArcList []int, machineNode *data.MachineNode, cost int)(int, []int, []int, error){
	// defer solver.timeCost("GetCostByRegretting", time.Now())
	regrettingCost, costForReverseArcList, machineTargerList, err := solver.GetMinCostAfterRegreting(machineNode, regretArcList)
	cost += regrettingCost
	return cost, costForReverseArcList, machineTargerList, err
}

/**
	if machineNode has tasks scheduled that mutexed with the task. we need to regret it, so we need to get min cost of regretting
 */
func (solver *MapSolver)GetCapacityAfterMutex(toNode *data.MachineNode, regretArcList []int)([]int){
	if len(regretArcList) == 0 {
		panic("regretArcList shouldn't be empty")
	}
	sumCapacity := make([]int, len(data.ArcList[regretArcList[0]].Capacity))

	for mutexArcIndex := 0; mutexArcIndex<len(regretArcList); mutexArcIndex++ { // the arc in the mutexArcList must has capacity
		sumCapacity = solver.AddFlow(sumCapacity, data.ArcList[regretArcList[mutexArcIndex]].Capacity)
	}
	sumCapacity = solver.AddFlow(sumCapacity, toNode.GetRightOutArcs()[0].Capacity)
	return sumCapacity
}


//func (solver *MapSolver)updatePath(gDist []int, gPre [][]int, gPath [][]int, cost int, toNodeIndex int, nodeIndex int,
//										outArcList []*data.Arc, i int, regretArcList []int, machineNode *data.MachineNode,
//											toNode data.NodeI, arcIndexList []int, costForReverseArcList []int){
//	// update endNode cost. remove all pre、path
//	gDist[data.EndNode.GetID()] = cost
//	gPre[data.EndNode.GetID()] = gPre[data.EndNode.GetID()][:0]
//	gPath[data.EndNode.GetID()] = gPath[data.EndNode.GetID()][:0]
//	// todo we also need to remove machineNode's(include scheduling machine and rescheduling machine ) pre and path
//	gPre[toNodeIndex] = gPre[toNodeIndex][:0]
//	gPath[toNodeIndex] = gPath[toNodeIndex][:0]
//
//	// todo we need to append
//	gDist[toNodeIndex] = cost
//	gPre[toNodeIndex] = append(gPre[toNodeIndex], nodeIndex)
//	gPath[toNodeIndex] = append(gPath[toNodeIndex], outArcList[i].ID)
//	// if there is no regretList
//	if regretArcList == nil { // if no arc need to be regret, we schedule the task to this machine
//
//		gPre[data.EndNode.GetID()] = append(gPre[data.EndNode.GetID()], machineNode.GetID())
//		gPath[data.EndNode.GetID()] = append(gPath[data.EndNode.GetID()], machineNode.GetRightOutArcs()[0].ID)
//		//continue
//	}else {
//		// todo we need to remove all reschedule machine pre and path.
//		for j,_ := range regretArcList {
//			machineNode :=  data.ArcList[arcIndexList[j]].DstNode.(*data.MachineNode)
//			gPre[machineNode.GetID()] = gPre[machineNode.GetID()][:0]
//			gPath[machineNode.GetID()] = gPath[machineNode.GetID()][:0]
//		}
//		preNode := toNode
//		for j,_ := range regretArcList {
//			// update reverseArc cost
//			data.ArcList[regretArcList[j]].Cost = costForReverseArcList[j]
//			// update templateNode
//			tempNode := data.ArcList[regretArcList[j]].DstNode
//			gDist[tempNode.GetID()] = gDist[preNode.GetID()] + data.ArcList[regretArcList[j]].Cost
//			gPre[tempNode.GetID()][0] = preNode.GetID()
//			gPath[tempNode.GetID()][0] = regretArcList[j]
//
//			// update machineNode, we need to append it.
//			machineNode :=  data.ArcList[arcIndexList[j]].DstNode.(*data.MachineNode)
//			gDist[machineNode.GetID()] = cost
//			gPre[machineNode.GetID()] = append(gPre[machineNode.GetID()], tempNode.GetID())
//			gPath[machineNode.GetID()] = append(gPath[machineNode.GetID()],  arcIndexList[j])
//
//			// update endNode, the machine node might be appended previously,so we need to avoid appending muti-times
//			// if pre has two src nodes, we can judge that the endNode has append this machineNode
//			if len(gPre[machineNode.GetID()]) == 1 {
//				gPre[data.EndNode.GetID()] = append(gPre[data.EndNode.GetID()], machineNode.GetID())
//				gPath[data.EndNode.GetID()] = append(gPath[data.EndNode.GetID()], machineNode.GetRightOutArcs()[0].ID)
//			}
//
//		}
//	}
//}

func (solver *MapSolver)timeCost(funcName string, start time.Time){
	terminal:=time.Since(start)
	if terminal.Seconds() > 1{
		fmt.Println(funcName+": "+terminal.String())
	}

}