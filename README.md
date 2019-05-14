# docker-icor
Dockerized iCOR - atmospheric correction plugin for SNAP

## Building

Download the iCOR installer for Linux https://info.vito.be/download-icor-submission and store in this directory.

Modify the name of the installer in the `Dockerfile`, if necessary.

```bash
docker build -t dhigras/icor
```

## Running

The container exposes the iCOR command-line interface. To see all the options, just run it.

```bash
docker run dhigras/icor
```
