//! Running a `lua:` task.
//!
//! `claviron-tasks` owns the trigger, the scope, the dispatch, the attempt
//! budget and the drain deadline. This owns one occurrence: the script is
//! read, run in a fresh state with this process's extensions, and dropped —
//! the way PHP runs a script. A changed script is simply the next occurrence's.
//!
//! Cancellation ends every wait the script is parked on with an error it can
//! see; the script decides how it finishes. A task's `graceful_timeout`, when
//! written, is the operator authorising the stop after it; unwritten, the
//! occurrence is waited for — `NO_IMPLICIT_TERMINATION`.

use std::path::PathBuf;
use std::sync::Arc;

use claviron_runtime::AppState;
use claviron_tasks::service::TaskService;
use claviron_tasks::{
    Error as TaskError, Executor, TaskExecution, TaskExecutorKind, TaskResult,
};

use crate::Script;
use crate::host::engine;

/// Install the Lua executor on the task service, if one is present.
pub fn register(state: &AppState) -> Result<(), String> {
    let Some(tasks) = state.registry_ref().resolve::<TaskService>() else {
        // A build with the engine but no task service is a valid composition.
        return Ok(());
    };
    tasks.set_executor(TaskExecutorKind::Lua, executor());
    Ok(())
}

fn executor() -> Executor {
    Arc::new(|execution: TaskExecution| Box::pin(run(execution)))
}

async fn run(execution: TaskExecution) -> TaskResult<()> {
    let TaskExecution {
        task, spec, state, cancellation, graceful_timeout, ..
    } = execution;
    let path = match (&spec.cwd, PathBuf::from(&spec.script).is_absolute()) {
        (Some(cwd), false) => PathBuf::from(cwd).join(&spec.script),
        _ => PathBuf::from(&spec.script),
    };
    let source = tokio::fs::read_to_string(&path).await.map_err(|error| {
        TaskError::other(format!(
            "lua task '{task}': {}: {error}",
            path.display()
        ))
    })?;
    let output = Arc::new(
        spec.log.open(spec.cwd.as_deref().map(std::path::Path::new), &task),
    );
    let script = Script {
        name: path.display().to_string(),
        source,
        args: spec.args.clone(),
        output: Some(Arc::new(move |bytes: &[u8]| {
            output.write_stdout(bytes);
        })),
    };

    let engine = engine(&state, &task).with_environment(spec.env.clone());
    let running = engine.execute(script, &cancellation);
    tokio::pin!(running);
    let outcome = tokio::select! {
        outcome = &mut running => outcome,
        () = cancellation.cancelled() => match graceful_timeout {
            Some(grace) => match tokio::time::timeout(grace, &mut running).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    tracing::warn!(
                        task = %task,
                        ?grace,
                        "Lua task did not finish within its graceful_timeout; stopping without it"
                    );
                    return Ok(());
                }
            },
            None => running.await
        }
    };
    outcome.map_err(|error| {
        TaskError::other(format!("lua task '{task}': {error}"))
    })
}
