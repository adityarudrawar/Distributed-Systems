import matplotlib.pyplot as plt

# Data for Linear consistency
linear_x = ['SET', 'GET', 'SET AND GET']
linear_y = [9.6444002866745, 10.120154695510864, 9.773513970375062]

# Data for Sequential consistency
sequential_x = ['SET', 'GET', 'SET AND GET']
sequential_y = [9.771539509296417, 1.0758793091773986, 3.4528001666069033]

# Data for Eventual consistency
eventual_x = ['SET', 'GET', 'SET AND GET']
eventual_y = [0.030848305225372314, 0.01655601501464844, 0.012655572891235351]

# Plotting the graph
fig, ax = plt.subplots()

ax.plot(linear_x, linear_y, label='Linear')
ax.plot(sequential_x, sequential_y, label='Sequential')
ax.plot(eventual_x, eventual_y, label='Eventual')

ax.set_xlabel('Type of Operation')
ax.set_ylabel('Time (s)')
ax.legend()

plt.show()
