# Caspian

Caspian is a controller for multi-cluster kubernetes environment that decides on scheduling and placement of workloads over clusters in such a way that the total carbon footprint generated by executing the workloads is minimized. Caspian lives in a hub cluster and through a multi-cluster management platform applies its placement and scheduling decisions on workloads to the destination spoke clusters.

![Caspina-architecture](https://github.com/sustainablecomputing/Caspian/assets/34821570/9ddebe06-732c-411a-9462-9effa59acd77)

Caspian works in a time slotted manner and has the following main components:
- **Carbon Monitoring:** It periodically fetches the predicted values of carbon intensity of spoke clusters.  
- **Green Scheduler:**   Based on the current and future values of carbon intensity, the status of workloads in the system, and available capacity of spoke clusters in the next T time slots, it calls its optimizer to obtain the best scheduling/placement for the workloads.  Once the Scheduler obtains the solution, it updates the spec of workloads to notify the multi-cluster manager about its decisions. 
- **Optimizer:** Contains an LP-based Algorithm to solve the carbon-aware scheduling/placement problem over multi clusters. Optimizer uses [clp package](https://github.com/lanl/clp) in its core. Clp  provides a Go interface to the COIN-OR Linear Programming (CLP) librarywhich is part of the [COIN-OR](https://www.coin-or.org/) suite.


## Caspian and MCAD
Caspian uses MicroMCAD as a Workload queueing and multi-cluster management platform to dispatch workloads to the destination clusters. A summary of the interactions between MicroMCAD and Caspian is depicted below.

![Caspian+MCAD](https://media.github.ibm.com/user/356384/files/6982b6a0-b3ae-4a44-b231-e1eab235963b)

##  Installation and Setup
