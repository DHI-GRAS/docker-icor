import os
import os.path
import sys
import logging
import zipfile
import tarfile
import shutil
import subprocess
import tempfile
import datetime
import glob


# -----------------------------------------------------------------------------

class PySparkLogger(object):
    def __init__(self, level=logging.NOTSET, stream=sys.stdout):
        self._level = level
        self._stream = stream
        self._str = ""

    def _format(self, level, msg, *args):
        s = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S ")
        s += logging.getLevelName(level)
        s += " "
        if args and len(args) > 0:
            s += msg % (args)
        else:
            s += msg
        s += "\n"
        return s

    def log(self, level, msg, *args):
        if level >= self._level:
            s = self._format(level, msg, *args)
            self._str += s
            if self._stream:
                self._stream.write(s)
                self._stream.flush()

    def debug(self, msg, *args):
        self.log(logging.DEBUG, msg, *args)

    def info(self, msg, *args):
        self.log(logging.INFO, msg, *args)

    def warning(self, msg, *args):
        self.log(logging.WARNING, msg, *args)

    def error(self, msg, *args):
        self.log(logging.ERROR, msg, *args)

    def __str__(self):
        return self._str

# -----------------------------------------------------------------------------


class SimpleContext(object):

    def __init__(self, params={}, logger=logging.getLogger()):
        self._params = params
        self._stagenr = 1
        self._logger = logger
        self.all_processes = []
        self.keep_tmp_list = []

    def copy_self(self):

        context = SimpleContext()

        context.setparams(self._params)
        context.setstagenr(self._stagenr)
        context.setlogger(self._logger)

        return context

    def setparams(self, params):
        for key in params:
            self.__setitem__(key, params[key])

    def setstagenr(self, stagenr):
        self._stagenr = stagenr

    def setlogger(self, logger):
        self._logger = logger

    def __setitem__(self, param, value):
        self._params[param] = value

    def __getitem__(self, param):
        return self._params[param]

    def _apply_params(self, s):
        return s.format(**self._params)

    def _add_to_env_path(self, env, key, param, sep=os.pathsep):
        path = self._params[param]
        path = path.replace(";", sep)
        if key in env:
            env[key] = env[key] + sep + path
        else:
            env[key] = path

    def _add_to_env(self, env, key, param):
        env[key] = self._params[param]

    def _fix_if_python_script(self, cmd, env):

        # If the first arg is a python script, replace it by the
        # 'python' command followed by the full script filename
        # as resolved using the PATH environment variable.

        exe, args = cmd.split(None, 1)

        if exe.endswith(".py"):
            path = env["PATH"]
            path = path.split(os.pathsep)

            for dir in path:
                file = os.path.join(dir, exe)

                if os.path.exists(file):
                    return "{python_command} " + file + " " + args

        return cmd

    def _shell(self, cmd, env):
        self._logger.info("Running %s", cmd)

        try:

            p = subprocess.Popen(cmd, shell=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

            (output, t) = p.communicate()

            self.all_processes.append(p)

        except subprocess.CalledProcessError as e:
            self._logger.error("Failed!")
            self._logger.error("Process output:")
            self._logger.error(" +" + "-" * 69)
            for line in e.output.splitlines():
                self._logger.error(" | " + line)
            self._logger.error(" +" + "-" * 69)

            raise e

        self._logger.debug("Done!")
        self._logger.debug("Process output:")
        self._logger.debug(" +" + "-" * 69)
        for line in output.splitlines():
            self._logger.debug(" | " + line)
        self._logger.debug(" +" + "-" * 69)

    def enter_stage(self, title):

        proc = (float(self._stagenr) / float(self._params["max_stages"])) * 100.0
        procent = "%3.0f" % proc
        self._logger.info("=" * 70)
        self._logger.info(procent + "% " + title)
        self._logger.info("=" * 70)
        self._stagenr += 1

    def resolve(self, path):
        actual_path = self._apply_params(path)

        self._logger.debug("Resolving " + path + " to " + actual_path)

        return actual_path

    def remove(self, path):
        path = self._apply_params(path)

        self._logger.debug("Removing %s", path)

        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path, True)
            else:
                os.remove(path)

    def rename(self, src_path, dst_path):
        src_path = self._apply_params(src_path)
        dst_path = self._apply_params(dst_path)

        self._logger.debug("Renaming %s to %s", src_path, dst_path)

        if os.path.exists(dst_path):
            if os.path.isdir(dst_path):
                shutil.rmtree(dst_path, True)
            else:
                os.remove(dst_path)

        os.rename(src_path, dst_path)

    def copy(self, src_path, dst_path):
        src_path = self._apply_params(src_path)
        dst_path = self._apply_params(dst_path)

        self._logger.debug("Copying %s to %s", src_path, dst_path)

        if not os.path.exists(dst_path):
            if not os.path.exists(os.path.dirname(dst_path)):
                os.makedirs(os.path.dirname(dst_path))
        else:
            if os.path.isdir(dst_path):
                shutil.rmtree(dst_path, True)
            else:
                os.remove(dst_path)

        shutil.copy(src_path, dst_path)

    def make_dir(self, path):
        path = self._apply_params(path)

        if os.path.exists(path):
            self.remove(path)

        self._logger.debug("Creating directory %s", path)

        os.makedirs(path)

    def unzip(self, path, dst_dir="."):
        path = self._apply_params(path)
        dst_dir = self._apply_params(dst_dir)

        self._logger.info("Unzipping %s", path)

        with zipfile.ZipFile(path, "r") as f:
            for name in f.namelist():
                if name[-1] not in "/\\":
                    self._logger.debug(" - %s", name)

                    f.extract(name, dst_dir)

    def untar(self, path, dst_dir="."):
        path = self._apply_params(path)
        dst_dir = self._apply_params(dst_dir)

        self._logger.info("Untarring %s", path)

        with tarfile.open(path, "r") as f:
            for name in f.getnames():
                if name[-1] not in "/\\":
                    self._logger.debug(" - %s", name)

                    f.extract(name, dst_dir)

    def unpack(self, path, dst_dir="."):
        path = self._apply_params(path)
        dst_dir = self._apply_params(dst_dir)

        if tarfile.is_tarfile(path):
            self.untar(path, dst_dir)
        elif zipfile.is_zipfile(path):
            self.unzip(path, dst_dir)
        else:
            raise "failed to unpack: " + path

    def invoke_gdal(self, cmd):

        # Prepare the environment for running GDAL tools

        env = os.environ.copy()

        self._add_to_env_path(env, "PATH", "gdal_path")
        self._add_to_env_path(env, "GDAL_DRIVER_PATH", "gdal_driver_path")
        self._add_to_env(env, "GDAL_DATA", "gdal_data")

        # Replace formatting parameters

        cmd = self._apply_params(cmd)

        # Call python explicitly for python scripts

        cmd = self._fix_if_python_script(cmd, env)

        # Now execute the specified shell command

        self._shell(cmd, env)

    def invoke_icor(self, cmd):

        # Prepare the environment for running iCOR tools

        env = os.environ.copy()

        self._add_to_env_path(env, "PATH", "icor_path")
        #self._add_to_env_path(env, "PATH", "gdal_path")
        self._add_to_env_path(env, "PYTHONPATH", "icor_pythonpath")
        #self._add_to_env_path(env, "GDAL_DRIVER_PATH", "gdal_driver_path")
        #self._add_to_env(env, "GDAL_DATA", "gdal_data")

        # Call python explicitly for python scripts

        cmd = self._fix_if_python_script(cmd, env)

        # Replace formatting parameters

        cmd = self._apply_params(cmd)

        # Now execute the specified shell command

        self._shell(cmd, env)

    def invoke_netcdf(self, cmd):

        # Prepare the environment for running iCOR tools

        env = os.environ.copy()

        self._add_to_env_path(env, "PATH", "netcdf_tools_path")

        # Call python explicitly for python scripts

        # Replace formatting parameters

        cmd = self._apply_params(cmd)

        # Now execute the specified shell command
        self._shell(cmd, env)

    def invoke_beam_idepix(self, cmd):

        cmd = "{beam_java_home}/bin/java {idepix_java_params}" + \
            " -Dceres.context=beam" + \
            " -Dbeam.mainClass=org.esa.beam.framework.gpf.main.GPT" + \
            " -Dbeam.home={beam_home}" + \
            " -Dncsa.hdf.hdflib.HDFLibrary.hdflib={beam_jhdf_lib}" + \
            " -Dncsa.hdf.hdf5lib.H5.hdf5lib={beam_jhdf5_lib}" + \
            " -jar {beam_home}/bin/ceres-launcher.jar" + \
            " " + cmd

        # Prepare the environment for running BEAM

        env = os.environ.copy()

        self._add_to_env_path(env, "PATH", "beam_path")
        self._add_to_env(env, "JAVA_HOME", "beam_java_home")
        self._add_to_env(env, "BEAM4_HOME", "beam_home")

        # Replace formatting parameters

        cmd = self._apply_params(cmd)

        # Now execute the specified shell command

        self._shell(cmd, env)

    def invoke_ac_runner(self, cmd):

        # Generate a temporary config file

        fd, conf_path = tempfile.mkstemp(".conf", "atcconf")

        os.close(fd)

        try:

            # Generate config file contents

            self.invoke_icor(
                "atcconf_generator/atcconf_generator.py" +
                " " + cmd +
                " -o " + conf_path
            )

            # Dump config file contents to log

            with open(conf_path) as f:
                self._logger.debug("Generated .conf:")
                self._logger.debug(" +" + "-" * 69)
                for line in f.read().splitlines():
                    self._logger.debug(" | " + line)
                self._logger.debug(" +" + "-" * 69)

            # Start processing

            self.invoke_icor("ac_runner " + conf_path)

        finally:
            os.remove(conf_path)

    def invoke_ac_runner_mine(self, cmd):

        # Generate a temporary config file

        fd, conf_path = tempfile.mkstemp(".conf", "atcconf")
        os.close(fd)

        try:

            # Generate config file contents
            cmd = self._apply_params(cmd)
            self.write_conf_ac_runner(cmd, conf_path)

            # Dump config file contents to log

            with open(conf_path) as f:
                self._logger.debug("Generated .conf:")
                self._logger.debug(" +" + "-" * 69)
                for line in f.read().splitlines():
                    self._logger.debug(" | " + line)
                self._logger.debug(" +" + "-" * 69)

            # Start processing

            self.invoke_icor("ac_runner " + conf_path)

        finally:
            os.remove(conf_path)

    def invoke_netcdf_tools(self, cmd):

        # Generate a temporary config file

        fd, conf_path = tempfile.mkstemp(".conf", "atcconf")
        os.close(fd)

        try:

            # Generate config file contents
            cmd = self._apply_params(cmd)
            self.write_conf_ac_runner(cmd, conf_path)

            # Dump config file contents to log

            with open(conf_path) as f:
                self._logger.debug("Generated .conf:")
                self._logger.debug(" +" + "-" * 69)
                for line in f.read().splitlines():
                    self._logger.debug(" | " + line)
                self._logger.debug(" +" + "-" * 69)

            # Start processing

            self.invoke_netcdf("netcdf_tools " + conf_path)

        finally:
            os.remove(conf_path)

    def invoke_ac_runner_check(self, cmd):

        # Generate a temporary config file
        env = os.environ.copy()

        fd, conf_path = tempfile.mkstemp(".conf", "atcconf")
        os.close(fd)

        try:

            # Generate config file contents
            cmd = self._apply_params(cmd)
            self.write_conf_ac_runner(cmd, conf_path)

            # Dump config file contents to log

            with open(conf_path) as f:
                self._logger.debug("Generated .conf:")
                self._logger.debug(" +" + "-" * 69)
                for line in f.read().splitlines():
                    self._logger.debug(" | " + line)
                self._logger.debug(" +" + "-" * 69)

            # Start processing

            self._add_to_env_path(env, "PATH", "icor_path")

            cmd = "ac_runner " + conf_path

        # Now execute the specified shell command

            self._logger.info("Running %s", cmd)

            try:

                p = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, env=env)

                self.all_processes.append(p)
                return 0

            except subprocess.CalledProcessError as e:
                self._logger.error("Failed!")
                self._logger.error("Process output:")
                self._logger.error(" +" + "-" * 69)
                for line in e.output.splitlines():
                    self._logger.error(" | " + line)
                self._logger.error(" +" + "-" * 69)

                raise Exception("process failed" + cmd)

            self._logger.debug("Done!")
            self._logger.debug("Process output:")
            self._logger.debug(" +" + "-" * 69)
            for line in output.splitlines():
                self._logger.debug(" | " + line)
            self._logger.debug(" +" + "-" * 69)

        finally:
            os.remove(conf_path)

    def invoke_project_gpt(self, cmd):
         # Generate a temporary config file
        env = os.environ.copy()

        fd, conf_path = tempfile.mkstemp(".conf", "atcconf")
        os.close(fd)

        try:

            cmd = "gpt " + cmd

        # Now execute the specified shell command

            self._logger.info("Running %s", cmd)

            try:

                p = subprocess.check_output(cmd, shell=True, env=env)

                self.all_processes.append(p)
                return 0

            except subprocess.CalledProcessError as e:
                self._logger.error("Failed!")
                self._logger.error("Process output:")
                self._logger.error(" +" + "-" * 69)
                for line in e.output.splitlines():
                    self._logger.error(" | " + line)
                self._logger.error(" +" + "-" * 69)

                return -1

            self._logger.debug("Done!")
            self._logger.debug("Process output:")
            self._logger.debug(" +" + "-" * 69)
            for line in output.splitlines():
                self._logger.debug(" | " + line)
            self._logger.debug(" +" + "-" * 69)

        finally:
            os.remove(conf_path)

    def make_temp_folder(sefl):
        path_name = tempfile.mkdtemp("_proc", "icor_")
        path_name = path_name.replace("\\", "/")
        return path_name

    def invoke_apply_gain_bias(self, cmd):

        # Create a temporary config file

        fd, conf_path = tempfile.mkstemp(".conf", "atcconf")

        os.close(fd)

        try:

            # Generate config file contents

            self.invoke_icor(
                "atcconf_generator/atcconf_generator.py" +
                " " + cmd +
                " -o " + conf_path
            )

            # Dump config file contents to log

            with open(conf_path) as f:
                self._logger.debug("Generated .conf:")
                self._logger.debug(" +" + "-" * 69)
                for line in f.read().splitlines():
                    self._logger.debug(" | " + line)
                self._logger.debug(" +" + "-" * 69)

            # Start processing

            self.invoke_icor(
                "ApplyGainOffset.py"
                " --config_file " + conf_path
            )

        finally:
            os.remove(conf_path)

    # some stuff

    def write_conf_ac_runner(self, cmd, conf_file):

        command = self.set_forward_slashes(cmd)

        fd = open(conf_file, 'w')
        fd.write(command)
        fd.flush()
        fd.close()

    def set_space_percent(self, cmd):
        return cmd.replace(" ", "%20")

    def set_forward_slashes(self, cmd):
        return cmd.replace("\\", "/")

    def write_config(self, folder):
        location = folder + "_config.log"
        f = open(location, "w")
        for key in self._params.keys():
            f.write(str(key) + "=" + str(self._params[key]) + "\n")
        f.close()
        self.add_keep_tmp(location)

    def add_keep_tmp(self, filelocation):
        self.keep_tmp_list.append(os.path.abspath(filelocation))

    def remove_tmp_files(self, folder, keep_tmp, target_folder=""):

        dir = os.getcwd()
        os.chdir(folder)
        files = glob.glob('*')

        for file in files:
            path_abs = os.path.abspath(folder + "/" + file)

            if not keep_tmp:
                os.remove(path_abs)
            elif not (path_abs in self.keep_tmp_list):
                os.remove(path_abs)
            else:
                dest = target_folder + "/" + str(file)
                self.copy(path_abs, dest)
                os.remove(path_abs)

        os.chdir(dir)
        os.removedirs(folder)
