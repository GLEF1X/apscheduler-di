# Implementation of dependency injection for `apscheduler`

### Prerequisites:

* `apscheduler-di` solves the problem since `apscheduler` doesn't support Dependency Injection
  natively, and it's real problem for developers to pass on complicated objects to jobs without
  corruptions

There is no some kind of monkeypatching, `apscheduler-di` just
implements [Decorator](https://en.wikipedia.org/wiki/Decorator_pattern) pattern and wraps up the
work of native `BaseScheduler` using [rodi](https://github.com/Neoteroi/rodi) lib

You can find an example of usage this library in `/examples` directory
