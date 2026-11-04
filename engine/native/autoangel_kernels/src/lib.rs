#![deny(unsafe_code)]

#[cfg(all(feature = "test-panic", not(debug_assertions)))]
compile_error!("the test-panic feature is forbidden in release builds");

/// Placeholder boundary for kernels promoted by the M0 benchmark.
///
/// Kernel algorithms intentionally do not live in this scaffold. They will be
/// added only after the benchmark records a promotion decision.
pub mod kernel {
    /// Identifies the scaffold library without retaining process state.
    pub const NAME: &str = "autoangel_kernels";
}

#[cfg(feature = "nif")]
mod nif {
    #[rustler::nif]
    fn scaffold() -> String {
        "ok".to_owned()
    }

    rustler::init!("Elixir.Autoangel.Kernel.Nif");
}

#[cfg(all(test, feature = "test-panic"))]
mod panic_tests {
    #[test]
    #[should_panic(expected = "autoangel test panic")]
    fn sentinel_panics_only_when_explicitly_enabled() {
        panic!("autoangel test panic");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_plain_library_module_without_a_nif() {
        assert_eq!(crate::kernel::NAME, "autoangel_kernels");
    }
}
