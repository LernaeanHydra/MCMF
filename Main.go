package main

import (
	"fmt"
	"errors"
	"container/list"
	"os"
	"bufio"
	"io"
	"strings"
	"strconv"

	"MCMF/util"
	"MCMF/initial"
	"MCMF/flowMap"
	"MCMF/data"
	"time"
)


/**
The project has two parts:
1. constuct flowMap with flowMap.txt file
	1.1 flowMap.txt includes [number of nodes],[number of edges] in the first line
	1.2 flowMap.txt includes each edge's content(src node, dst node, capacity, cost) in following lines
2. find all paths with MCMF algrithm
	2.1 find currently min cost path with SPFA algrithm and add this path to result
	2.2 if no incremental path exsits, return. Or continue finding SPFA path
 */




type Edge struct  {
	to int  // node edge points to
	vol int   // capacity left can be used
	cost int  // cost per unit flow
	next int  // index of next edge having the same source node
}


var gEdges []Edge  // all edges object
var gHead []int  // one of the edge with the same source node
var gPre []int  // previous node before node i of the chosen min cost path
var gPath []int  // edge which have dest node i of chosen min cost path
var gDist []int  // the min cost from src node to i node

var gEdgeCount int // used in constructing flowMap to record edge's index
var edgeNum int  // number of edges
var nodeNum int  // number of nodes



func main() {
	//err := constructMap()  // construct flowMap with flowMap.txt
	//if(err != nil) {
	//	panic(err)
	//}
	//result, cost := mcmf(0, nodeNum-1)  // result representing all path. each path was representing by a series of node index along path
	//fmt.Println("cost:")
	//fmt.Println(cost)
	//fmt.Println("result:")
	//fmt.Println(result)

	fd,_:=os.OpenFile("result2.log",os.O_RDWR|os.O_CREATE|os.O_APPEND,0644)

	startTime1_1 := time.Now().UnixNano()
	factory := util.DBReaderFactory{Url:"47.104.16.133", Database:"cluster"}
	reader := factory.GetDBReader("mongo")
	reader.GetMachines()
	reader.GetTasks()
	endTime1_2 := time.Now().UnixNano()

	fd.Write([]byte("transfer data from dataBase: "))
	fd.Write([]byte(strconv.FormatInt(endTime1_2-startTime1_1, 10)))
	fd.Write([]byte("\n"))

	startTime2_1 := time.Now().UnixNano()
	if mongoReader, ok := reader.(*util.MongoReader); ok {
		defer mongoReader.Session.Close()
	}else {
		panic("type revert failed")
	}
	initial.InitGlobal()
	initial.InitResource()

	builder := flowMap.MapBuilder{}
	builder.BuildMap()
	endTime2_2 := time.Now().UnixNano()

	fd.Write([]byte("constructMap: "))
	fd.Write([]byte(strconv.FormatInt(endTime2_2-startTime2_1, 10)))
	fd.Write([]byte("\n"))

	startTime3_1 := time.Now().UnixNano()
	solver := flowMap.MapSolver{}
	result, cost, flow := solver.GetMCMF(data.StartNode.GetID(), data.EndNode.GetID())

	endTime3_2 := time.Now().UnixNano()

	fd.Write([]byte("solve map: "))
	fd.Write([]byte(strconv.FormatInt(endTime3_2-startTime3_1, 10)))
	fd.Write([]byte("\n"))


	fd.Close()
	fmt.Println("result:")
	for key, val := range result{
		fmt.Print("["+key+", "+strconv.Itoa(val)+"]")
		fmt.Print(" ")
	}
	fmt.Println("cost:")
	fmt.Println(cost)
	fmt.Println("flow:")
	fmt.Println(flow)






}

/**
 1. accept input data from flowMap.txt
 2. initialize gHead, gEdges
 3. insertEdge between node with u,v,vol,cost given by input data
 */
func constructMap()(error){
	mapData := make([]string, 0)
	f,err := os.Open("map.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)

	/*
	add all flowMap data to memory.
	check data format
	initialize gHead and gEdges
	*/
	for {
		line, err := rd.ReadString('\n')  // here hint that there must be a "\n" at the end of file
		if err != nil || io.EOF == err {
			break
		}
		mapData = append(mapData, strings.Replace(line, "\n", "", -1))  // remove "\n" and add to mapData
	}

	if len(mapData) < 2 {
		return errors.New("输入文件格式有误，图内容过少")
	}

	numberSlice := strings.Split(mapData[0], " ")  // 1st line of file indicates [number of nodes] and [numbers of edges]
	if len(numberSlice) != 2 {
		return errors.New("输入文件格式有误，第一行只能有两个数字")
	}
	nodeNum, err = strconv.Atoi(numberSlice[0])
	if err != nil {
		panic(err)
	}
	edgeNum, err = strconv.Atoi(numberSlice[1])
	if err != nil {
		panic(err)
	}
	if len(mapData) != edgeNum+1 {
		return errors.New("输入文件格式有误，总行数和有向边数量不符")
	}

	gHead = make([]int, nodeNum)  // initialize gHead
	for i:=0; i<len(gHead); i ++ {
		gHead[i] = -1  // default -1
	}
	gEdges = make([]Edge, edgeNum*2)  // initialize gEdges, including reverse edge, so mutiplied by 2

	/* start to insert edge */
	gEdgeCount = 0
	for i := 1; i < len(mapData); i ++ {
		line := mapData[i]
		edgeSlice := strings.Split(line, " ")
		if len(edgeSlice) != 4 {
			return errors.New("输入文件格式有误，有向边只能用四元组进行表示")
		}
		u, err := strconv.Atoi(edgeSlice[0])
		v, err := strconv.Atoi(edgeSlice[1])
		vol, err := strconv.Atoi(edgeSlice[2])
		cost, err := strconv.Atoi(edgeSlice[3])
		if err != nil{
			panic(err)
		}
		insertEdge(u, v, vol, cost)
	}
	return nil
}

/**
insert edge from u to v with capacity and cost
it also insert edge from v to u with zero capacity and -cost
two edge have adjecent index in gEdges[]
 */
func insertEdge(u int, v int, vol int, cost int){
	gEdges[gEdgeCount].to = v
	gEdges[gEdgeCount].vol = vol
	gEdges[gEdgeCount].cost = cost
	gEdges[gEdgeCount].next = gHead[u]
	gHead[u] = gEdgeCount
	gEdgeCount ++

	gEdges[gEdgeCount].to = u
	gEdges[gEdgeCount].vol = 0
	gEdges[gEdgeCount].cost = -cost
	gEdges[gEdgeCount].next = gHead[v]
	gHead[v] = gEdgeCount  // here hints that the reverse edge has odd index
	gEdgeCount ++
}


func mcmf(s int, t int)([][]int, int){
	result := make([][]int, 0)
	cost := 0
	flow := 0
	for spfa(s, t)  { // if there has incremental path
		tmp_result := make([]int, 0)
		f := int(^uint(0) >> 1) // infinite value
		for u := t; u != s; u = gPre[u] {  //  visit all node from dst node to src node in the path
			if gEdges[gPath[u]].vol < f {
				f = gEdges[gPath[u]].vol  // find the min capacity of this path and this is the flow of this path
			}
			tmp_result = append(tmp_result, u)
		}
		tmp_result = append(tmp_result, s)
		result = append(result, tmp_result)

		flow += f
		cost += gDist[t] * f
		for u := t; u != s; u = gPre[u] {  // //  visit all node from dst node to src node in the path
			gEdges[gPath[u]].vol -= f  // the capacity of edges along this path update its capacity
			gEdges[gPath[u]^1].vol += f  // the capacity of reverse edges along this path update its capacity
		}
	}
	return result, cost
}

/**
s : source node
t : destination node
return whether there is min cost path or not
 */
func spfa(s int, t int)(bool){
	gPre = make([]int, nodeNum)  // initialize gPre
	for i:=0; i<len(gPre); i ++ {
		gPre[i] = -1  // default -1
	}

	gPath = make([]int, nodeNum)  // initialize gPath
	for i:=0; i<len(gPath); i ++ {
		gPath[i] = -1  // default -1
	}

	gDist = make([]int, nodeNum) // initialize gDist
	for i:=0; i<len(gDist); i ++ {
		gDist[i] = int(^uint(0) >> 1)  // default INT_MAX
	}

	gDist[s] = 0
	Q := list.New()  // Q is the loose queue, it record all node , from which the min cost to another node may change
	Q.PushBack(s)
	for Q.Len() > 0  {
		u_element := Q.Front()
		Q.Remove(u_element)
		u, err := u_element.Value.(int)
		if(!err){
			panic(err)
		}
		for e := gHead[u]; e != -1; e = gEdges[e].next {  // visit all edges has node u as their src node
			v := gEdges[e].to
			if gEdges[e].vol > 0 && gDist[u]+gEdges[e].cost < gDist[v] {  // if edge e has availiable capacity and node v's current min cost is more than that from node u to v
				gDist[v] = gDist[u] + gEdges[e].cost  // update node v's min cost
				gPre[v] = u
				gPath[v] = e
				Q.PushBack(v)  // because v's min cost has changed, so we need to check if the nodes that node v can reach can change its min cost
			}
		}
	}

	if gPre[t] == -1 {
		return false
	}
	return true

}