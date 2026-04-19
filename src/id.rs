use std::fmt;

macro_rules! define_id {
    ($name:ident) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
        pub struct $name(pub u32);

        impl $name {
            pub const fn raw(self) -> u32 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}#{}", stringify!($name), self.0)
            }
        }
    };
}

define_id!(AccountId);
define_id!(SecurityId);
define_id!(SleeveId);
define_id!(LotId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_compare_equal_on_same_raw() {
        assert_eq!(AccountId(3), AccountId(3));
        assert_ne!(AccountId(3), AccountId(4));
    }

    #[test]
    fn display_includes_type_name() {
        assert_eq!(format!("{}", SecurityId(7)), "SecurityId#7");
    }

    #[test]
    fn ids_are_copy_and_cheap() {
        let a = SleeveId(1);
        let b = a;
        let c = a;
        assert_eq!(a, b);
        assert_eq!(b, c);
    }
}
