import argparse
parser = argparse.ArgumentParser(description="Example argparse script with multiple arguments")
parser.add_argument("input_file", help="Path to input file")
parser.add_argument("output_file", help="Path to output file")
parser.add_argument("-t", "--threshold", type=float, default=0.5, help="Threshold value for processing (default: 0.5)")
parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose mode")
parser.add_argument("-a", "--algorithm", choices=["algorithm1", "algorithm2", "algorithm3"], default="algorithm1", help="Choose processing algorithm (default: algorithm1)")


def main(args):
    print("Received arguments:")
    print(f"Input file: {args.input_file}")
    print(f"Output file: {args.output_file}")
    print(f"Threshold: {args.threshold}")
    print(f"Verbose mode: {args.verbose}")
    print(f"Algorithm: {args.algorithm}")

args = parser.parse_args()
main(args)
