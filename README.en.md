# SuperAgent Dispatch

[简体中文](README.md) · **English**

**SuperAgent aims to answer a simple question: when several tools share a job, who took it, how far did they get, and where is the result?** Dispatch is its task coordinator. Your project submits a task, a tool that can handle it claims and runs it, and Dispatch records the status and result locally so you can check what happened and pick up where the work stopped.

For example, if you're sorting a collection of media, one script can make a shortlist and another tool can process the selected items. If work gets stuck, you can see what finished, what still needs attention, and where to find the results you already have.

## Where to start

| What you want to see | Start here |
| --- | --- |
| How to hand out tasks, track progress, and find results on one computer | The Dispatch v0.1.0 self-test below |
| How a workflow can connect research, sample script generation, and simulated review | [Try the M0 preview](https://github.com/hjh939325136-alt/superagent-dispatch/blob/preview/m0-source/README.en.md) |

This repository provides source code and local examples. v0.1.0 covers single-machine coordination and sample handoffs. The M0 preview demonstrates a longer workflow. Production rollout, cross-device execution, continuous operation, and migration of real databases still need separate validation.

## Quick start

Use Python 3.11 or later. Open a terminal in the extracted or cloned repository and run the first command. The second runs the optional regression suite.

```sh
python -B -m sa_dispatch selftest
python -B -m unittest discover -s tests -v
```

The self-test runs built-in sample jobs and checks that their results can be read back from exported task history. It uses SQLite in a temporary directory and needs no separate database service.

## Connect your own script

Start with the [sample content adapter](sa_dispatch_hotspot/__init__.py). Dispatch writes a JSON request file, and your script returns a JSON response with the same `request_id`. The [v0.1.0 adapter reference](https://github.com/hjh939325136-alt/superagent-dispatch/blob/v0.1.0/README.md#use-a-project-adapter) explains registration, duplicate requests, and what to do when the result of a write is uncertain.

See the [v0.1.0 source](https://github.com/hjh939325136-alt/superagent-dispatch/tree/v0.1.0), [Linux deployment example](deploy/README.md), and [development updates](https://github.com/hjh939325136-alt/superagent-dispatch/blob/preview/m0-source/DEVELOPMENT_UPDATES.en.md).
