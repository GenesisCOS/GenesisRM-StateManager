<script>
export default {
  data() {
    return {
      workloads: [
        {name: "Test"}
      ]
    };
  },
  methods: {
    refresh() {
      console.log("refresh locust");
    }
  }
}
</script>

<template>
  <div>
    <table class="table">
      <thead>
        <tr>
          <th scope="col">#</th>
          <th scope="col">Name</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(workload, index) in workloads">
          <th scope="row">{{ index }}</th>
          <th>{{ workload.name }}</th>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<style>
/*
@media (min-width: 1024px) {
  .about {
    min-height: 100vh;
    display: flex;
    align-items: center;
  }
}
*/
</style>
