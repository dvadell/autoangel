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

    /// Names of kernels promoted by the M0 benchmark.
    ///
    /// This must remain empty while the Elixir implementations are selected.
    /// A future promotion must add the Rust implementation and its determinism
    /// property test in the same change.
    pub const PROMOTED_KERNELS: &[&str] = &[];
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
    use proptest::prelude::*;

    #[test]
    fn exposes_plain_library_module_without_a_nif() {
        assert_eq!(crate::kernel::NAME, "autoangel_kernels");
    }

    // Task 4.3 is intentionally skipped because M0 promoted no kernels.
    // This guard prevents a future promotion from silently bypassing the
    // required Rust score-bounds property.
    #[test]
    fn score_bounds_property_is_skipped_without_promoted_kernels() {
        assert!(crate::kernel::PROMOTED_KERNELS.is_empty());
    }

    // **Validates: Requirements 49.2, 47.6**
    // Property 46: Kernel calls are deterministic (Rust mirror).
    // M0 promoted no kernels, so this guard is the appropriate test until a
    // promoted implementation exists. A future promotion must replace the
    // guard with a property that invokes that kernel twice for each input.
    proptest! {
        #[test]
        fn determinism_property_is_skipped_without_promoted_kernels(
            _input in any::<u64>()
        ) {
            prop_assert!(
                crate::kernel::PROMOTED_KERNELS.is_empty(),
                "a promoted kernel requires a Rust determinism property test"
            );
        }
    }
}
