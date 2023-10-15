pub mod arbiter_pool;
pub mod connection;

pub mod prelude {
  //! The `maxwell-utils` prelude.
  //!
  //! The purpose of this module is to alleviate imports of many common maxwell-utils
  //! types by adding a glob import to the top of maxwell-utils heavy modules:
  //!
  //! ```
  //! use maxwell_utils::prelude::*;
  //! ```
  pub use triomphe::Arc;

  pub use crate::arbiter_pool::*;
  pub use crate::connection::*;
}
