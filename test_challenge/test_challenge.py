from test_utilities import run_experiment

if __name__ == "__main__":
    flflow = run_experiment()

    from metaflow import Metaflow, Flow, Task, Step

    m=Metaflow()
    f=Flow('TestFlow').latest_run
    run_id = flflow._run_id
    print(f"run_id: {run_id}")
    s = Step(f'TestFlow/{run_id}/train')
    print(s)
    t = Task(f'TestFlow/{run_id}/train/3')
    print(t)
    print(t.data.optimizer)
    print(t.data.model)
