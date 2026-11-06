//! The engine a host of this process builds: every extension whose state is
//! here now. Tasks and applications get the same one.

use std::sync::Arc;

use claviron_runtime::AppState;

use crate::Engine;

/// The engine a script or an application of this process gets. `name` is
/// what its log records are targeted at.
pub fn engine(
    state: &AppState,
    #[cfg_attr(not(feature = "log"), allow(unused_variables))] name: &str,
) -> Engine {
    let path = state
        .registry_ref()
        .resolve::<crate::config::LuaConfig>()
        .map(|config| config.path())
        .unwrap_or_default();
    #[allow(unused_mut)]
    let mut engine = Engine::new()
        .with_path(path)
        .with(Arc::new(crate::JsonExtension))
        .with(Arc::new(crate::RegexExtension))
        .with(Arc::new(crate::RouterExtension))
        .with(Arc::new(crate::process::ProcessExtension::new(
            state.run_mode(),
        )));
    #[cfg(feature = "orbit")]
    if let Some(cells) = state
        .registry_ref()
        .resolve::<claviron_orbit::CellsProvider>()
        .and_then(|provider| provider.handle())
    {
        engine = engine.with(Arc::new(crate::orbit::CellExtension::new(cells)));
    }
    #[cfg(feature = "orbit")]
    if let Some(streams) = state
        .registry_ref()
        .resolve::<claviron_orbit::StreamsProvider>()
        .and_then(|provider| provider.handle())
    {
        engine =
            engine.with(Arc::new(crate::stream::StreamExtension::new(streams)));
    }
    #[cfg(feature = "orbit")]
    {
        engine = engine.with(Arc::new(crate::events::EventsExtension::new(
            state.events().bus().clone(),
        )));
        if let Some(bus) = state
            .registry_ref()
            .resolve::<claviron_orbit::InvocationProvider>()
            .and_then(|provider| provider.handle())
        {
            engine =
                engine.with(Arc::new(crate::invoke::InvokeExtension::new(bus)));
        }
    }
    #[cfg(feature = "fetch")]
    if let Some(fetcher) = state
        .registry_ref()
        .resolve::<crate::config::LuaConfig>()
        .and_then(|config| config.fetcher())
    {
        engine =
            engine.with(Arc::new(crate::fetch::FetchExtension::new(fetcher)));
    }
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    if let Some(service) =
        state.registry_ref().resolve::<claviron_db::DbService>()
    {
        engine = engine.with(Arc::new(crate::db::DbExtension::new(service)));
    }
    #[cfg(feature = "log")]
    {
        let channels = state
            .registry_ref()
            .resolve::<claviron_logging::pipeline::LogChannels>();
        engine = engine
            .with(Arc::new(crate::log::LogExtension::new(channels, name)));
    }
    engine
}
