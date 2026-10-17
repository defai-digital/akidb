# Example service recovery

The synthetic Cedar service uses port 7410 for its health endpoint.
Before restarting Cedar, save the current request count and inspect its readiness response.
After restarting Cedar, wait for readiness before sending normal traffic.
