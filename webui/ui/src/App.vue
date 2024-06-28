<script setup>
import { RouterLink, RouterView } from 'vue-router'
//import HelloWorld from './components/HelloWorld.vue'
</script>

<template>
  <!-- <header>
    <img alt="Vue logo" class="logo" src="@/assets/logo.svg" width="125" height="125" />

    <div class="wrapper">
      <HelloWorld msg="You did it!" />

      <nav>
        <RouterLink to="/">Home</RouterLink>
        <RouterLink to="/about">About</RouterLink>
        <RouterLink to="/test">Test</RouterLink>
      </nav>
    </div>
  </header>

  <RouterView /> -->
  <div class="offcanvas offcanvas-start w-25" tabindex="-1" id="offcanvas" data-bs-keyboard="false" data-bs-backdrop="false">
    <div class="offcanvas-header">
      <h6 class="offcanvas-title d-none d-sm-block" id="offcanvas">
        GenesisRM
      </h6>
      <button type="button" class="btn-close text-reset" data-bs-dismiss="offcanvas" aria-label="Close"></button>
    </div>
    <div class="offcanvas-body px-0">
      <ul class="nav nav-pills flex-column mb-sm-auto mb-0 align-items-start" id="menu">
        <li class="nav-item">
          <a href="#" class="nav-link text-truncate">
            <i class="fs-5 bi-house"></i>
            <span class="ms-1 d-none d-sm-inline">
              <RouterLink to="/">Home</RouterLink>
            </span>
          </a>
        </li>
        <li>
          <a href="#submenu1" data-bs-toggle="collapse" class="nav-link text-truncate">
            <i class="fs-5 bi-speedometer2"></i>
            <span class="ms-1 d-none d-sm-inline">
              <RouterLink to="/about">About</RouterLink>
            </span>
          </a>
        </li>
        <li>
          <a href="#" class="nav-link text-truncate">
            <i class="fs-5 bi-table"></i>
            <span class="ms-1 d-none d-sm-inline">Orders</span>
          </a>
        </li>
        <li class="dropdown">
          <a href="#" class="nav-link dropdown-toggle text-truncate" id="dropdown" data-bs-toggle="dropdown" aria-expanded="false">
            <i class="fs-5 bi-bootstrap"></i>
            <span class="ms-1 d-none d-sm-inline">Components</span>
          </a>
          <ul class="dropdown-menu text-small shadow" aria-labelledby="dropdown">
            <li>
              <a class="dropdown-item" href="#">
                <RouterLink to="/components/locust">Locust</RouterLink>
              </a>
            </li>
            <li><a class="dropdown-item" href="#">Service</a></li>
            <li>
              <hr class="dropdown-divider">
            </li>
            <li><a class="dropdown-item" href="#">Autoscaler</a></li>
          </ul>
        </li>
      </ul>
    </div>
  </div>
  <nav class="navbar navbar-expand-lg bg-body-tertiary">
    <div class="container-fluid">
      <a class="navbar-brand" href="#">GenesisRM</a>
      <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav me-auto mb-2 mb-lg-0">
          <!-- <li class="nav-item">
            <a class="nav-link active" aria-current="page" href="#">Home</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="#">Link</a>
          </li>
          <li class="nav-item dropdown">
            <a class="nav-link dropdown-toggle" href="#" role="button" data-bs-toggle="dropdown" aria-expanded="false">
              Dropdown
            </a>
            <ul class="dropdown-menu">
              <li><a class="dropdown-item" href="#">Action</a></li>
              <li><a class="dropdown-item" href="#">Another action</a></li>
              <li><hr class="dropdown-divider"></li>
              <li><a class="dropdown-item" href="#">Something else here</a></li>
            </ul>
          </li>
          <li class="nav-item">
            <a class="nav-link disabled" aria-disabled="true">Disabled</a>
          </li> -->
          <li class="nav-item">
            <a class="nav-link float-end" data-bs-toggle="offcanvas" data-bs-target="#offcanvas" role="button">
              侧边栏
            </a>
          </li>
        </ul>
        <!-- <form class="d-flex" role="search">
          <input class="form-control me-2" type="search" placeholder="Search" aria-label="Search">
          <button class="btn btn-outline-success" type="submit">Search</button>
        </form> -->
      </div>
    </div>
  </nav>
  <div class="container-fluid">
    <div class="row">
      <div class="col min-vh-100 py-3">
        <RouterView />
      </div>
    </div>
  </div>
</template>

<style scoped>

/*
header {
  line-height: 1.5;
  max-height: 100vh;
}

.logo {
  display: block;
  margin: 0 auto 2rem;
}

nav {
  width: 100%;
  font-size: 12px;
  text-align: center;
  margin-top: 2rem;
}

nav a.router-link-exact-active {
  color: var(--color-text);
}

nav a.router-link-exact-active:hover {
  background-color: transparent;
}

nav a {
  display: inline-block;
  padding: 0 1rem;
  border-left: 1px solid var(--color-border);
}

nav a:first-of-type {
  border: 0;
}

@media (min-width: 1024px) {
  header {
    display: flex;
    place-items: center;
    padding-right: calc(var(--section-gap) / 2);
  }

  .logo {
    margin: 0 2rem 0 0;
  }

  header .wrapper {
    display: flex;
    place-items: flex-start;
    flex-wrap: wrap;
  }

  nav {
    text-align: left;
    margin-left: -1rem;
    font-size: 1rem;

    padding: 1rem 0;
    margin-top: 1rem;
  }
}
  */
</style>
