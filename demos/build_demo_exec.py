from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))



from logixbase.compiler import BuildProject


if __name__ == "__main__":
    self = BuildProject("Demoproject", r"D:\licx\codes\logixbase\demos\configs\compiler.ini", r"d:\logs")
    self.start()
    self.stop()
