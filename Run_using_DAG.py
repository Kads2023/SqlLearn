# Welcome to your new notebook
# Type here in the cell editor to add code!
DAG = {
    "activities": [
        {
            "name": "Notebook 1", # activity name, must be unique
            "path": "Notebook 1", # notebook path
            "timeoutPerCellInSeconds": 90, # max timeout for each cell, default to 90 seconds
            #"args": {"p1": "changed value", "p2": 100}, # notebook parameters
        },
        
        {
            "name": "Notebook 2",
            "path": "Notebook 2",
            "timeoutPerCellInSeconds": 120,
            #"args": {"p1": "changed value 3", "p2": 300},
            #"retry": 1,
            #"retryIntervalInSeconds": 10,
            "dependencies": ["Notebook 1"] # list of activity names that this activity depends on
        }
    ]
}
mssparkutils.notebook.runMultiple(DAG)
