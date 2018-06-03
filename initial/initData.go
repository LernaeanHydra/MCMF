package initial

import (
	"MCMF/data"
)

/**
	1. initial CurrentAppMap
	2. initial ApplicationAndTask
	3. initial
 */
func InitGlobal(){
	for k,task := range data.CurrentTaskList {  // GO LANGUAGE TIPS: if you delete data when you range slice, the slice while loop as time as it should be, but when the index > its actually length, it get its final data for those index
		if index, ok := data.CurrentAppMap[task.AppGroup]; !ok { // if this application hasn't existed
			data.CurrentAppMap[task.AppGroup] = len(data.CurrentAppMap)  // indicate this app's index in ApplicationAndTask
			//data.CurrentAppList = append(data.CurrentAppList, data.Application{Name:task.AppGroup})// add this application to currentAppList
			// add this task to applicationAndTask

			tmpTaskList := make([]*data.Task, 0)
			tmpTaskList = append(tmpTaskList, &data.CurrentTaskList[k])
			tmpTasks := make([][]*data.Task,0)
			tmpTasks = append(tmpTasks, tmpTaskList)
			data.ApplicationAndTask = append(data.ApplicationAndTask, tmpTasks)
		} else { 											// if this application already exist, add this task to applicationAndTask[index]
			isExist := false
			for i, taskList := range data.ApplicationAndTask[index]{
				if task.Cpu == taskList[0].Cpu&&
					task.Mem == taskList[0].Mem&&
						task.DiskSize == taskList[0].DiskSize&&
							hasSameContent(task.MatchTag, taskList[0].MatchTag)&&
								hasSameContent(task.ExcludeTag, taskList[0].ExcludeTag)&&
									hasSameContent(task.ExclusiveTag, taskList[0].ExclusiveTag)&&
										task.IsKeyInstance == taskList[0].IsKeyInstance {
											isExist = true
											data.ApplicationAndTask[index][i] = append(data.ApplicationAndTask[index][i], &data.CurrentTaskList[k])
											break
				}
			}
			if !isExist {
				tmpTaskList := make([]*data.Task, 0)
				tmpTaskList = append(tmpTaskList, &data.CurrentTaskList[k])
				data.ApplicationAndTask[index] = append(data.ApplicationAndTask[index], tmpTaskList)
			}
		}

	}

}


/**
	1. initial Task's TaskRejectedList
	2. initial Machine's TaskAcceptedList
 */
func InitResource(){
	// 1. initial Task's TaskRejectedList
	//mutexLists := make(map[string][]int, 0)  // sort out all task by tasks' exclusive tag. store task's index in CurrentTaskList
	//for i, v := range data.CurrentTaskList {
	//	tags := v.ExclusiveTag
	//	for _, tag := range tags{
	//		if _, ok := mutexLists[tag]; !ok { // if this tag hasn't existed
	//			tmpList := make([]int, 0)
	//			tmpList = append(tmpList, i)
	//			mutexLists[tag] = tmpList
	//		}else { 							  // if this tag exists, add this task index to its mutexList
	//			mutexLists[tag] = append(mutexLists[tag], i)
	//		}
	//	}
	//}
	//
	//for _, val := range mutexLists { // all tasks' index collection that have one same exclusive tag
	//	for _, taskIndex := range val { // for every task's index in this collection
	//		task := data.CurrentTaskList[taskIndex] // get this task
	//		for i:=0; i<len(val); i++ {
	//			if val[i] != taskIndex{
	//				// check if this task has existed in TaskRejectedList
	//				isContained := false
	//				for _, rejectedTask := range task.TaskRejectedList {
	//					if rejectedTask == &data.CurrentTaskList[val[i]]{
	//						isContained = true
	//						break
	//					}
	//				}
	//				if isContained {
	//					continue
	//				}
	//				data.CurrentTaskList[taskIndex].TaskRejectedList = append(data.CurrentTaskList[taskIndex].TaskRejectedList, &data.CurrentTaskList[val[i]])
	//			}
	//		}
	//	}
	//}

	// 2. initial Machine's TaskAcceptedList
	for index, _ := range data.ClusterMachineList{
		for index2, _ := range data.ApplicationAndTask{
			appAccepted := make([][]*data.Task, 0)
			for index3, _ := range data.ApplicationAndTask[index2] {
				templateAccepted := make([]*data.Task, 0)
				if isAccepted(&data.ClusterMachineList[index], data.ApplicationAndTask[index2][index3][0]) {
					templateAccepted = append(templateAccepted, data.ApplicationAndTask[index2][index3]...)
				}
				if len(templateAccepted) >0 {
					appAccepted = append(appAccepted, templateAccepted)
				}
			}
			if len(appAccepted) > 0 {
				data.ClusterMachineList[index].TaskAcceptedList = append(data.ClusterMachineList[index].TaskAcceptedList, appAccepted)
			}
		}
	}

}

func isAccepted(machine *data.Machine, task *data.Task) bool{
	if contain(machine.MatchTag, task.MatchTag)&&contain(task.ExcludeTag, machine.ExcludeTag) {
		return true
	}else {
		return false
	}
}

func contain(s []string, d []string) bool {
	for _, dVal := range d{
		isContained := false
		for _, sVal := range s{
			if sVal == dVal {
				isContained = true
				break
			}
		}
		if !isContained {
			return false
		}
	}
	return true
}

func hasSameContent(s []string, d []string) bool {
	if len(s) != len(d){
		return false
	}
	for _, val1 := range s{
		hasIt := false
		for _, val2 := range d{
			if val1 == val2{
				hasIt = true
				continue
			}
		}
		if hasIt == false{
			return false
		}
	}
	return true
}