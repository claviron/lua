use std::sync::Arc;

use claviron_lua::{
    Execution, Extension, mlua::Lua, process::ProcessExtension,
};
use claviron_runtime::{RunMode, WorkerId};

#[test]
fn host_process_metadata_preserves_worker_and_non_worker_roles() {
    let lua = Lua::new();
    let execution = Arc::new(Execution::default());
    for (mode, role, worker_id) in [
        (RunMode::Worker(WorkerId::new(4)), "worker", Some(4_u8)),
        (RunMode::Master, "master", None),
        (RunMode::Standalone, "standalone", None),
    ] {
        let extension = ProcessExtension::new(mode);
        assert_eq!(extension.name(), "claviron.process");
        assert_eq!(extension.global(), Some("process"));
        let module = extension.module(&lua, &execution).unwrap();
        assert_eq!(module.get::<String>("role").unwrap(), role);
        assert_eq!(module.get::<Option<u8>>("worker_id").unwrap(), worker_id);
        module.set("worker_id", 99).unwrap();
        let next = extension.module(&lua, &execution).unwrap();
        assert_eq!(next.get::<Option<u8>>("worker_id").unwrap(), worker_id);
    }
}
