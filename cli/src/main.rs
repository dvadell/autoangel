fn main() {
    println!("autoangel CLI placeholder");
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]
        #[test]
        fn generated_values_are_stable(value in any::<u8>()) {
            prop_assert_eq!(value, value);
        }
    }
}
