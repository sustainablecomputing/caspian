# Scheduler
Scheduler is periodically called by Caspian. It first gets the info of spoke clusters and characjteristics of Appwrappers. It then calls the optimizer to find scheduling/placement for appwrappers. Finally, it updates target cluster and dispatching gates of appwrappers notifying MCAD about its decisions.  The figure below shows the steps that the scheduler takes to update Appwrappers. 

<img width="741" alt="scheduler" src="https://github.com/sustainablecomputing/caspian/assets/34821570/c565d033-4955-4525-b93a-83828eadf213">
